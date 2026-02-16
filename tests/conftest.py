from ZODB.blob import Blob
from ZODB.FileStorage import FileStorage
from ZODB.MappingStorage import MappingStorage

import os
import pytest
import shutil
import tempfile
import transaction
import ZODB


@pytest.fixture
def temp_dir():
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d)


@pytest.fixture
def source_filestorage(temp_dir):
    path = os.path.join(temp_dir, "source.fs")
    blob_dir = os.path.join(temp_dir, "source_blobs")
    fs = FileStorage(path, blob_dir=blob_dir)
    yield fs
    fs.close()


@pytest.fixture
def dest_filestorage(temp_dir):
    path = os.path.join(temp_dir, "dest.fs")
    blob_dir = os.path.join(temp_dir, "dest_blobs")
    fs = FileStorage(path, blob_dir=blob_dir)
    yield fs
    fs.close()


@pytest.fixture
def populated_source(source_filestorage):
    """Source FileStorage with 3 transactions: basic data, nested data, blob."""
    db = ZODB.DB(source_filestorage)
    conn = db.open()
    root = conn.root()

    # Txn 1: basic data
    root["key1"] = "value1"
    root["key2"] = 42
    txn = transaction.get()
    txn.setUser("user1")
    txn.note("First transaction")
    txn.commit()

    # Txn 2: nested data
    root["key3"] = {"nested": [1, 2, 3]}
    txn = transaction.get()
    txn.setUser("user2")
    txn.note("Second transaction")
    txn.commit()

    # Txn 3: blob
    root["blob1"] = Blob(b"Hello, blob world!")
    txn = transaction.get()
    txn.setUser("user3")
    txn.note("Third transaction with blob")
    txn.commit()

    conn.close()
    db.close()
    return source_filestorage


@pytest.fixture
def source_mapping_storage():
    return MappingStorage()


@pytest.fixture
def dest_mapping_storage():
    return MappingStorage()
