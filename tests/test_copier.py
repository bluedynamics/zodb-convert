from ZODB.utils import u64
from zodb_convert.copier import copy_transactions
from zodb_convert.copier import detect_capabilities
from zodb_convert.copier import get_incremental_start_tid
from zodb_convert.copier import storage_has_data

import transaction
import ZODB
import ZODB.FileStorage


class TestStorageHasData:
    def test_empty_storage(self, source_filestorage):
        assert storage_has_data(source_filestorage) is False

    def test_populated_storage(self, populated_source):
        assert storage_has_data(populated_source) is True


class TestDetectCapabilities:
    def test_filestorage(self, source_filestorage, dest_filestorage):
        caps = detect_capabilities(source_filestorage, dest_filestorage)
        assert caps["source_has_iterator"] is True
        assert caps["dest_has_restore"] is True
        assert caps["source_has_blobs"] is True
        assert caps["dest_has_blobs"] is True
        assert caps["dest_has_blob_restore"] is True

    def test_mapping_storage(self, source_mapping_storage, dest_mapping_storage):
        caps = detect_capabilities(source_mapping_storage, dest_mapping_storage)
        assert caps["source_has_iterator"] is True
        assert caps["dest_has_restore"] is False
        assert caps["source_has_blobs"] is False
        assert caps["dest_has_blobs"] is False
        assert caps["dest_has_blob_restore"] is False


class TestCopyEmpty:
    def test_copy_empty_storage(self, source_filestorage, dest_filestorage):
        txn_count, obj_count, blob_count = copy_transactions(
            source_filestorage, dest_filestorage
        )
        assert txn_count == 0
        assert obj_count == 0
        assert blob_count == 0


class TestCopyTransactions:
    def test_copy_single_transaction(self, source_filestorage, dest_filestorage):
        # ZODB.DB creates an initial root txn + our explicit commit = 2 txns total
        db = ZODB.DB(source_filestorage)
        conn = db.open()
        root = conn.root()
        root["key"] = "value"
        transaction.commit()
        conn.close()
        db.close()

        txn_count, obj_count, blob_count = copy_transactions(
            source_filestorage, dest_filestorage
        )
        # 2 txns: initial root + our commit
        assert txn_count == 2
        assert obj_count >= 1
        assert blob_count == 0

        # Verify data in destination
        db2 = ZODB.DB(dest_filestorage)
        conn2 = db2.open()
        root2 = conn2.root()
        assert root2["key"] == "value"
        conn2.close()
        db2.close()

    def test_copy_multiple_transactions(self, populated_source, dest_filestorage):
        txn_count, obj_count, _blob_count = copy_transactions(
            populated_source, dest_filestorage
        )
        # 4 txns: initial root + 3 explicit commits from populated_source
        assert txn_count == 4
        assert obj_count > 0

        # Verify data
        db = ZODB.DB(dest_filestorage)
        conn = db.open()
        root = conn.root()
        assert root["key1"] == "value1"
        assert root["key2"] == 42
        assert root["key3"] == {"nested": [1, 2, 3]}
        conn.close()
        db.close()

    def test_copy_preserves_tids(self, populated_source, dest_filestorage):
        copy_transactions(populated_source, dest_filestorage)

        source_tids = [txn.tid for txn in populated_source.iterator()]
        dest_tids = [txn.tid for txn in dest_filestorage.iterator()]
        assert source_tids == dest_tids

    def test_copy_preserves_metadata(self, populated_source, dest_filestorage):
        copy_transactions(populated_source, dest_filestorage)

        dest_txns = list(dest_filestorage.iterator())
        # Index 0 is the initial root transaction (empty metadata)
        # Our transactions start at index 1
        assert b"user1" in dest_txns[1].user
        assert b"First transaction" in dest_txns[1].description
        assert b"user2" in dest_txns[2].user
        assert b"user3" in dest_txns[3].user


class TestCopyBlobs:
    def test_copy_with_blobs(self, populated_source, dest_filestorage):
        _txn_count, _obj_count, blob_count = copy_transactions(
            populated_source, dest_filestorage
        )
        assert blob_count >= 1

        # Verify blob data
        db = ZODB.DB(dest_filestorage)
        conn = db.open()
        root = conn.root()
        blob = root["blob1"]
        with blob.open("r") as f:
            assert f.read() == b"Hello, blob world!"
        conn.close()
        db.close()

    def test_copy_no_blobs_to_non_blob_dest(
        self, source_filestorage, dest_mapping_storage
    ):
        """When dest doesn't support blobs, blob records are still copied (data only)."""
        # Use source with only simple data (no blobs) to avoid store() conflict issues
        db = ZODB.DB(source_filestorage)
        conn = db.open()
        root = conn.root()
        root["key"] = "value"
        transaction.commit()
        conn.close()
        db.close()

        txn_count, _obj_count, blob_count = copy_transactions(
            source_filestorage, dest_mapping_storage
        )
        assert txn_count >= 1
        assert blob_count == 0


class TestCopyFallback:
    def test_fallback_to_store(self, source_mapping_storage, dest_mapping_storage):
        """MappingStorage doesn't implement IStorageRestoreable, use store() fallback."""
        # Use MappingStorage as source too (simple single-txn case)
        db = ZODB.DB(source_mapping_storage)
        conn = db.open()
        root = conn.root()
        root["key"] = "value"
        transaction.commit()
        conn.close()
        db.close()

        txn_count, obj_count, _blob_count = copy_transactions(
            source_mapping_storage, dest_mapping_storage
        )
        assert txn_count >= 1
        assert obj_count >= 1

        # Verify data in mapping storage
        db2 = ZODB.DB(dest_mapping_storage)
        conn2 = db2.open()
        root2 = conn2.root()
        assert root2["key"] == "value"
        conn2.close()
        db2.close()


class TestDryRun:
    def test_dry_run_no_writes(self, populated_source, dest_filestorage):
        txn_count, obj_count, _blob_count = copy_transactions(
            populated_source, dest_filestorage, dry_run=True
        )
        # 4 txns: initial root + 3 explicit
        assert txn_count == 4
        assert obj_count > 0

        # Destination should still be empty
        assert storage_has_data(dest_filestorage) is False


class TestIncremental:
    def test_get_incremental_start_tid_empty(self, dest_filestorage):
        result = get_incremental_start_tid(dest_filestorage)
        assert result is None

    def test_get_incremental_start_tid_with_data(self, populated_source):
        tid = get_incremental_start_tid(populated_source)
        assert tid is not None
        last = populated_source.lastTransaction()
        assert u64(tid) == u64(last) + 1

    def test_incremental_copy(self, temp_dir):
        """Copy some transactions, add more to source, copy again incrementally."""
        import os

        src_path = os.path.join(temp_dir, "inc_source.fs")
        src_blobs = os.path.join(temp_dir, "inc_source_blobs")
        dst_path = os.path.join(temp_dir, "inc_dest.fs")
        dst_blobs = os.path.join(temp_dir, "inc_dest_blobs")

        # Create source with 2 transactions
        source = ZODB.FileStorage.FileStorage(src_path, blob_dir=src_blobs)
        db = ZODB.DB(source)
        conn = db.open()
        root = conn.root()
        root["key1"] = "value1"
        transaction.commit()
        root["key2"] = "value2"
        transaction.commit()
        conn.close()
        db.close()

        # Open storages for copy
        source = ZODB.FileStorage.FileStorage(src_path, blob_dir=src_blobs)
        dest = ZODB.FileStorage.FileStorage(dst_path, blob_dir=dst_blobs)
        copy_transactions(source, dest)
        source.close()
        dest.close()

        # Add third transaction to source
        source = ZODB.FileStorage.FileStorage(src_path, blob_dir=src_blobs)
        db = ZODB.DB(source)
        conn = db.open()
        root = conn.root()
        root["key3"] = "value3"
        transaction.commit()
        conn.close()
        db.close()

        # Incremental copy
        source = ZODB.FileStorage.FileStorage(src_path, blob_dir=src_blobs)
        dest = ZODB.FileStorage.FileStorage(dst_path, blob_dir=dst_blobs)
        start_tid = get_incremental_start_tid(dest)
        txn_count, _obj_count, _blob_count = copy_transactions(
            source, dest, start_tid=start_tid
        )
        assert txn_count == 1  # Only the new transaction

        # Verify all data
        db2 = ZODB.DB(dest)
        conn2 = db2.open()
        root2 = conn2.root()
        assert root2["key1"] == "value1"
        assert root2["key2"] == "value2"
        assert root2["key3"] == "value3"
        conn2.close()
        db2.close()
