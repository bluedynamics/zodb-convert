"""Upload deferred blobs from a manifest file."""

from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor

import contextlib
import functools
import logging
import os
import threading
import time


logger = logging.getLogger(__name__)

_DEFAULT_MAX_RETRIES = 3
_DEFAULT_RETRY_BASE_DELAY = 2.0


def _read_manifest(manifest_path):
    """Parse the manifest TSV into (blob_path, s3_key, zoid, size) tuples."""
    entries = []
    with open(manifest_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) != 4:
                logger.warning("Skipping malformed manifest line: %s", line)
                continue
            blob_path, s3_key, zoid_str, size_str = parts
            entries.append((blob_path, s3_key, int(zoid_str), int(size_str)))
    return entries


def _upload_one(
    blob_path,
    s3_key,
    zoid,
    size,
    *,
    s3_client,
    max_retries,
    retry_base_delay,
    cleanup,
    shutdown,
):
    """Upload one blob with retries.  Returns 'uploaded', 'failed' or 'skipped'."""
    if not os.path.exists(blob_path):
        logger.warning("Blob file missing, skipping: %s (oid=0x%016x)", blob_path, zoid)
        return "skipped"
    last_exc = None
    for attempt in range(max_retries):
        if shutdown.is_set():
            return "failed"
        try:
            s3_client.upload_file(blob_path, s3_key)
            if cleanup:
                with contextlib.suppress(OSError):
                    os.unlink(blob_path)
            return "uploaded"
        except Exception as exc:
            last_exc = exc
            if shutdown.is_set():
                return "failed"
            if attempt < max_retries - 1:
                delay = retry_base_delay * (2**attempt)
                logger.warning(
                    "Upload oid=0x%016x attempt %d/%d failed (%s), "
                    "retrying in %.0fs ...",
                    zoid,
                    attempt + 1,
                    max_retries,
                    exc,
                    delay,
                )
                # Interruptible sleep — wakes immediately on shutdown.
                shutdown.wait(delay)
    logger.error(
        "Upload oid=0x%016x FAILED after %d attempts: %s",
        zoid,
        max_retries,
        last_exc,
    )
    return "failed"


def upload_from_manifest(
    manifest_path,
    s3_client,
    workers=8,
    max_retries=_DEFAULT_MAX_RETRIES,
    retry_base_delay=_DEFAULT_RETRY_BASE_DELAY,
    cleanup=False,
):
    """Read manifest TSV and upload blobs to S3.

    Returns dict with counts: uploaded, failed, skipped.
    """
    entries = _read_manifest(manifest_path)
    total = len(entries)
    logger.info("Manifest: %d blob(s) to upload with %d workers", total, workers)

    counts = {"uploaded": 0, "failed": 0, "skipped": 0}

    # Shutdown signal — checked by retry loops to avoid sleeping during teardown.
    shutdown = threading.Event()

    upload = functools.partial(
        _upload_one,
        s3_client=s3_client,
        max_retries=max_retries,
        retry_base_delay=retry_base_delay,
        cleanup=cleanup,
        shutdown=shutdown,
    )

    t0 = time.time()
    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(upload, *entry): entry[1] for entry in entries}
            for fut in as_completed(futures):
                counts[fut.result()] += 1

                # Progress logging every 100 completions.
                done = sum(counts.values())
                if done % 100 == 0 or done == total:
                    elapsed = time.time() - t0
                    rate = done / elapsed if elapsed > 0 else 0
                    logger.info(
                        "Progress: %d/%d (%.0f/s) — %d uploaded, %d failed, %d skipped",
                        done,
                        total,
                        rate,
                        counts["uploaded"],
                        counts["failed"],
                        counts["skipped"],
                    )
    except KeyboardInterrupt:
        logger.warning("Interrupted — signalling workers to stop ...")
        shutdown.set()
        # Pool __exit__ will wait for running futures (but retry sleeps
        # wake immediately via shutdown.wait).

    elapsed = time.time() - t0
    logger.info(
        "Manifest upload complete: %d uploaded, %d failed, %d skipped (%.1fs)",
        counts["uploaded"],
        counts["failed"],
        counts["skipped"],
        elapsed,
    )
    return counts
