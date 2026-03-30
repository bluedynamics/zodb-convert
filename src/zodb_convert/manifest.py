"""Upload deferred blobs from a manifest file."""

from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor

import contextlib
import logging
import os
import time


logger = logging.getLogger(__name__)

_DEFAULT_MAX_RETRIES = 3
_DEFAULT_RETRY_BASE_DELAY = 2.0


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

    logger.info("Manifest: %d blob(s) to upload", len(entries))

    uploaded = 0
    failed = 0
    skipped = 0

    def _upload_one(blob_path, s3_key, zoid, size):
        if not os.path.exists(blob_path):
            logger.warning(
                "Blob file missing, skipping: %s (oid=0x%016x)", blob_path, zoid
            )
            return "skipped"
        last_exc = None
        for attempt in range(max_retries):
            try:
                s3_client.upload_file(blob_path, s3_key)
                if cleanup:
                    with contextlib.suppress(OSError):
                        os.unlink(blob_path)
                return "uploaded"
            except Exception as exc:
                last_exc = exc
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
                    time.sleep(delay)
        logger.error(
            "Upload oid=0x%016x FAILED after %d attempts: %s",
            zoid,
            max_retries,
            last_exc,
        )
        return "failed"

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_upload_one, *entry): entry[1] for entry in entries}
        for fut in as_completed(futures):
            result = fut.result()
            if result == "uploaded":
                uploaded += 1
            elif result == "failed":
                failed += 1
            elif result == "skipped":
                skipped += 1

    elapsed = time.time() - t0
    logger.info(
        "Manifest upload complete: %d uploaded, %d failed, %d skipped (%.1fs)",
        uploaded,
        failed,
        skipped,
        elapsed,
    )
    return {"uploaded": uploaded, "failed": failed, "skipped": skipped}
