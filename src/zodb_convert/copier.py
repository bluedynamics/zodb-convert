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


def get_incremental_start_tid(source, destination):
    """Get the TID to resume from for incremental copy.

    Returns None if destination is empty (full copy needed).

    When the destination contains TIDs beyond the source range (e.g. from
    ZODB.DB root object creation with wall-clock timestamps), scans the
    destination to find the actual last restored source TID.
    """
    if not storage_has_data(destination):
        return None

    dest_last = destination.lastTransaction()
    source_last = source.lastTransaction()
    dest_last_int = u64(dest_last) if isinstance(dest_last, bytes) else dest_last
    source_last_int = (
        u64(source_last) if isinstance(source_last, bytes) else source_last
    )

    if dest_last_int <= source_last_int:
        # Normal case: destination TIDs are within source range
        return p64(dest_last_int + 1)

    # Destination has TIDs beyond source (e.g. from ZODB.DB initialization).
    # Scan destination to find the highest TID within the source range.
    log.info("Destination has TIDs beyond source range, scanning for resume point...")
    last_valid_int = None
    for txn in destination.iterator():
        tid_int = u64(txn.tid)
        if tid_int <= source_last_int:
            last_valid_int = tid_int

    if last_valid_int is None:
        return None  # No source TIDs in destination, full copy

    return p64(last_valid_int + 1)


def _try_parallel_delegation(
    source, destination, workers, start_tid=None, blob_mode="inline"
):
    """Try delegating to the destination's parallel copyTransactionsFrom.

    Returns (txn_count, obj_count, blob_count) on success, or None if the
    destination doesn't support the *workers* parameter.  No hard dependency
    on any specific storage implementation — pure duck typing.
    """
    copy_method = getattr(destination, "copyTransactionsFrom", None)
    if copy_method is None:
        log.info("Destination has no copyTransactionsFrom, using generic copier.")
        return None

    kwargs = {"workers": workers}
    if start_tid is not None:
        kwargs["start_tid"] = start_tid
    if blob_mode != "inline":
        kwargs["blob_mode"] = blob_mode

    try:
        log.info(
            "Delegating to destination.copyTransactionsFrom(%s).",
            ", ".join(f"{k}={v!r}" for k, v in kwargs.items()),
        )
        copy_method(source, **kwargs)
    except TypeError:
        # Destination's copyTransactionsFrom doesn't accept these kwargs.
        log.info(
            "Destination doesn't support parallel copy, "
            "falling back to generic sequential copier."
        )
        return None

    # Destination handled everything including its own progress logging.
    return None, None, None


def _load_blob_filename(source, record, caps):
    """Return the source blob file for *record*, or None if there is none.

    is_blob_record() is a fast pre-filter (cheap byte scan) to avoid
    expensive loadBlob() stat calls on non-blob records.  The blob count
    is based on loadBlob() success, not the filter.
    """
    data = record.data
    if not (
        data
        and caps["source_has_blobs"]
        and caps["dest_has_blobs"]
        and is_blob_record(data)
    ):
        return None
    try:
        return source.loadBlob(record.oid, record.tid)
    except (KeyError, OSError):
        return None  # No blob file data stored for this oid/tid
    except Exception:
        log.warning(
            "Failed to load blob for oid=%s tid=%s, copying record only",
            record.oid,
            record.tid,
        )
        return None


def _copy_record(
    record, tid, txn_info, source, destination, caps, preindex, temp_blobs
):
    """Copy one data record within the currently open destination transaction.

    Returns (byte_size, is_blob).
    """
    oid = record.oid
    data = record.data
    byte_size = len(data) if data else 0

    blob_filename = _load_blob_filename(source, record, caps)
    if blob_filename is None:
        if caps["dest_has_restore"]:
            destination.restore(oid, record.tid, data, "", record.data_txn, txn_info)
        else:
            destination.store(oid, preindex.get(oid), data, "", txn_info)
            preindex[oid] = tid
        return byte_size, False

    # Copy blob to temp file in destination's temp dir
    fd, tmp_path = tempfile.mkstemp(
        prefix="zodbconvert_", suffix=".tmp", dir=destination.temporaryDirectory()
    )
    os.close(fd)
    shutil.copy2(blob_filename, tmp_path)
    temp_blobs.append(tmp_path)
    byte_size += os.path.getsize(blob_filename)

    if caps["dest_has_blob_restore"]:
        destination.restoreBlob(
            oid, record.tid, data, tmp_path, record.data_txn, txn_info
        )
    else:
        destination.storeBlob(oid, preindex.get(oid), data, tmp_path, "", txn_info)
        preindex[oid] = tid
    return byte_size, True


def _count_dry_run(txn_info, progress):
    """Count one transaction's records without writing.  Returns the count."""
    oids = [record.oid for record in txn_info]
    if progress:
        progress.on_transaction(txn_info.tid, len(oids), 0, 0, oids=oids)
    return len(oids)


def _rebase_preindex(preindex, tid, committed_tid):
    """After a store() fallback commit, map this txn's oids to the committed TID."""
    for oid in list(preindex):
        if preindex[oid] == tid:
            preindex[oid] = committed_tid


def _cleanup_temp_blobs(temp_blobs):
    """Delete temp blob files, ignoring already-gone ones, and clear the list."""
    for tmp in temp_blobs:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
    temp_blobs.clear()


def copy_transactions(
    source,
    destination,
    start_tid=None,
    dry_run=False,
    progress=None,
    workers=1,
    blob_mode="inline",
):
    """Copy transactions from source to destination storage.

    Uses IStorageIteration.iterator() on source.
    Uses IStorageRestoreable.restore() on destination if available,
    otherwise falls back to store().

    When *workers* > 1, attempts to delegate to the destination's
    ``copyTransactionsFrom(source, workers=N)`` for parallel writing.
    This requires no hard dependency — if the destination doesn't
    support the *workers* keyword, falls back to sequential copy.

    Returns (txn_count, obj_count, blob_count).
    """
    if workers > 1 and not dry_run:
        result = _try_parallel_delegation(
            source, destination, workers, start_tid, blob_mode
        )
        if result is not None:
            return result
        # Fall through to generic sequential copier.

    caps = detect_capabilities(source, destination)

    if not caps["source_has_iterator"]:
        raise ValueError("Source storage does not support IStorageIteration")

    restoring = caps["dest_has_restore"]

    # For store() fallback: track previous serial per oid
    preindex = {}

    fiter = source.iterator(start=start_tid)
    txn_count = 0
    obj_count = 0
    blob_count = 0
    temp_blobs = []

    in_tpc = False  # Track whether a TPC transaction is in progress
    txn_info = None

    try:
        for txn_info in fiter:
            tid = txn_info.tid

            if dry_run:
                obj_count += _count_dry_run(txn_info, progress)
                txn_count += 1
                continue

            # Begin transaction on destination with original TID
            if restoring:
                destination.tpc_begin(txn_info, tid, txn_info.status)
            else:
                destination.tpc_begin(txn_info)
            in_tpc = True

            txn_byte_size = 0
            txn_blobs = 0
            txn_oids = []

            for record in txn_info:
                txn_oids.append(record.oid)
                byte_size, is_blob = _copy_record(
                    record,
                    tid,
                    txn_info,
                    source,
                    destination,
                    caps,
                    preindex,
                    temp_blobs,
                )
                txn_byte_size += byte_size
                txn_blobs += int(is_blob)
                obj_count += 1

            destination.tpc_vote(txn_info)
            committed_tid = destination.tpc_finish(txn_info)
            in_tpc = False
            txn_count += 1
            blob_count += txn_blobs

            # For store() fallback: update preindex with actual committed TID
            if not restoring and committed_tid:
                _rebase_preindex(preindex, tid, committed_tid)

            _cleanup_temp_blobs(temp_blobs)

            if progress:
                progress.on_transaction(
                    tid, len(txn_oids), txn_byte_size, txn_blobs, oids=txn_oids
                )

    finally:
        # Abort any in-flight TPC transaction
        if in_tpc:
            with contextlib.suppress(Exception):
                destination.tpc_abort(txn_info)
        if hasattr(fiter, "close"):
            fiter.close()
        _cleanup_temp_blobs(temp_blobs)

    return txn_count, obj_count, blob_count
