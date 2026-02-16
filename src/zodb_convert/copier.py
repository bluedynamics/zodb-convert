"""Core copy logic for ZODB storage conversion."""

from ZODB.blob import is_blob_record
from ZODB.interfaces import IBlobStorage
from ZODB.interfaces import IBlobStorageRestoreable
from ZODB.interfaces import IStorageIteration
from ZODB.interfaces import IStorageRestoreable
from ZODB.utils import p64
from ZODB.utils import u64

import contextlib
import logging
import os
import shutil
import tempfile


log = logging.getLogger("zodb-convert")


def storage_has_data(storage):
    """Check if a storage contains any transactions."""
    it = storage.iterator()
    try:
        try:
            next(it)
        except (IndexError, StopIteration):
            return False
        return True
    finally:
        if hasattr(it, "close"):
            it.close()


def detect_capabilities(source, destination):
    """Detect what interfaces source and destination storages provide."""
    return {
        "source_has_iterator": IStorageIteration.providedBy(source),
        "source_has_blobs": IBlobStorage.providedBy(source),
        "dest_has_restore": IStorageRestoreable.providedBy(destination),
        "dest_has_blob_restore": IBlobStorageRestoreable.providedBy(destination),
        "dest_has_blobs": IBlobStorage.providedBy(destination),
    }


def get_incremental_start_tid(destination):
    """Get the TID to resume from for incremental copy.

    Returns None if destination is empty.
    """
    if not storage_has_data(destination):
        return None
    last_tid = destination.lastTransaction()
    if isinstance(last_tid, bytes):
        last_tid = u64(last_tid)
    return p64(last_tid + 1)


def copy_transactions(
    source, destination, start_tid=None, dry_run=False, progress=None
):
    """Copy transactions from source to destination storage.

    Uses IStorageIteration.iterator() on source.
    Uses IStorageRestoreable.restore() on destination if available,
    otherwise falls back to store().

    Returns (txn_count, obj_count, blob_count).
    """
    caps = detect_capabilities(source, destination)

    if not caps["source_has_iterator"]:
        raise ValueError("Source storage does not support IStorageIteration")

    restoring = caps["dest_has_restore"]
    blob_restoring = caps["dest_has_blob_restore"]
    source_has_blobs = caps["source_has_blobs"]
    dest_has_blobs = caps["dest_has_blobs"]

    # For store() fallback: track previous serial per oid
    preindex = {}

    fiter = source.iterator(start=start_tid)
    txn_count = 0
    obj_count = 0
    blob_count = 0
    temp_blobs = []

    try:
        for txn_info in fiter:
            tid = txn_info.tid

            if dry_run:
                rec_count = 0
                for _record in txn_info:
                    rec_count += 1
                obj_count += rec_count
                txn_count += 1
                if progress:
                    progress.on_transaction(tid, rec_count, 0, 0)
                continue

            # Begin transaction on destination with original TID
            if restoring:
                destination.tpc_begin(txn_info, tid, txn_info.status)
            else:
                destination.tpc_begin(txn_info)

            txn_byte_size = 0
            txn_blobs = 0

            for record in txn_info:
                oid = record.oid
                data = record.data

                # Handle blob records
                blob_filename = None
                if (
                    data
                    and source_has_blobs
                    and dest_has_blobs
                    and is_blob_record(data)
                ):
                    try:
                        blob_filename = source.loadBlob(oid, record.tid)
                    except Exception:
                        log.warning(
                            "Failed to load blob for oid=%s tid=%s, copying record only",
                            oid,
                            record.tid,
                        )

                if blob_filename is not None:
                    # Copy blob to temp file in destination's temp dir
                    tmp_dir = destination.temporaryDirectory()
                    fd, tmp_path = tempfile.mkstemp(
                        prefix="zodbconvert_", suffix=".tmp", dir=tmp_dir
                    )
                    os.close(fd)
                    shutil.copy2(blob_filename, tmp_path)
                    temp_blobs.append(tmp_path)
                    txn_byte_size += os.path.getsize(blob_filename)

                    if blob_restoring:
                        destination.restoreBlob(
                            oid, record.tid, data, tmp_path, record.data_txn, txn_info
                        )
                    else:
                        pre = preindex.get(oid)
                        destination.storeBlob(oid, pre, data, tmp_path, "", txn_info)
                        preindex[oid] = tid
                    txn_blobs += 1
                elif restoring:
                    destination.restore(
                        oid, record.tid, data, "", record.data_txn, txn_info
                    )
                else:
                    pre = preindex.get(oid)
                    destination.store(oid, pre, data, "", txn_info)
                    preindex[oid] = tid

                if data:
                    txn_byte_size += len(data)
                obj_count += 1

            destination.tpc_vote(txn_info)
            committed_tid = destination.tpc_finish(txn_info)
            txn_count += 1
            blob_count += txn_blobs

            # For store() fallback: update preindex with actual committed TID
            if not restoring and committed_tid:
                for oid in list(preindex):
                    if preindex[oid] == tid:
                        preindex[oid] = committed_tid

            # Clean up temp blob files
            for tmp in temp_blobs:
                with contextlib.suppress(OSError):
                    os.unlink(tmp)
            temp_blobs.clear()

            if progress:
                progress.on_transaction(tid, obj_count, txn_byte_size, txn_blobs)

    finally:
        if hasattr(fiter, "close"):
            fiter.close()
        # Clean any remaining temp blobs
        for tmp in temp_blobs:
            with contextlib.suppress(OSError):
                os.unlink(tmp)

    return txn_count, obj_count, blob_count
