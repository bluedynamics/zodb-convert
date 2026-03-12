"""Progress reporting for ZODB storage conversion."""

from ZODB.utils import readable_tid_repr

import logging
import time


log = logging.getLogger("zodb-convert")


def _format_bytes(n):
    """Format byte count as human-readable string."""
    if n < 1024:
        return f"{n} B"
    if n < 1024 * 1024:
        return f"{n / 1024:.1f} KB"
    if n < 1024 * 1024 * 1024:
        return f"{n / (1024 * 1024):.1f} MB"
    return f"{n / (1024 * 1024 * 1024):.1f} GB"


def _format_duration(seconds):
    """Format seconds as human-readable duration."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    secs = seconds % 60
    if minutes < 60:
        return f"{minutes}m {secs:.0f}s"
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours}h {mins}m"


class ProgressReporter:
    """Progress reporter for ZODB storage conversion.

    Uses total_oids (from len(source), O(1) for FileStorage) for approximate
    progress percentage based on unique OIDs seen so far.

    Tier 1: Per-transaction logging in verbose mode.
    Tier 2: Interval-based logging (every 10s or 100 txns).
    Tier 3: Summary at completion.
    """

    def __init__(self, total_oids=0, verbose=False, log_interval=10, log_count=100):
        self.total_oids = total_oids
        self.verbose = verbose
        self.log_interval = log_interval
        self.log_count = log_count

        self.txn_count = 0
        self.obj_count = 0
        self.blob_count = 0
        self.total_bytes = 0
        self._seen_oids = set()

        self.start_time = time.monotonic()
        self.last_log_time = self.start_time
        self.last_log_txn_count = 0

    def _pct(self):
        if self.total_oids and self._seen_oids:
            return len(self._seen_oids) * 100.0 / self.total_oids
        return 0

    def on_transaction(self, tid, record_count, byte_size, blob_count, oids=()):
        """Called after each transaction is copied."""
        self.txn_count += 1
        self.obj_count += record_count
        self.blob_count += blob_count
        self.total_bytes += byte_size
        self._seen_oids.update(oids)

        now = time.monotonic()
        is_first = self.txn_count == 1

        if self.verbose or is_first:
            self._log_transaction(tid, record_count, blob_count, byte_size)
        elif self._should_interval_log(now):
            self._log_interval(now)
            self.last_log_time = now
            self.last_log_txn_count = self.txn_count

    def _should_interval_log(self, now):
        elapsed_since_log = now - self.last_log_time
        txns_since_log = self.txn_count - self.last_log_txn_count
        return (
            elapsed_since_log >= self.log_interval or txns_since_log >= self.log_count
        )

    def _log_transaction(self, tid, record_count, blob_count, byte_size):
        elapsed = time.monotonic() - self.start_time
        pct = self._pct()
        pct_str = f" ~{pct:.1f}%" if pct else ""

        eta = ""
        if pct > 0 and elapsed > 0:
            remaining = elapsed * (100 - pct) / pct
            eta = f", ETA: {_format_duration(remaining)}"

        log.info(
            "TX %s%s tid=%s %d records, %d blobs, %s%s",
            self.txn_count,
            pct_str,
            readable_tid_repr(tid),
            record_count,
            blob_count,
            _format_bytes(byte_size),
            eta,
        )

    def _log_interval(self, now):
        elapsed = now - self.start_time
        txn_rate = self.txn_count / elapsed if elapsed > 0 else 0
        byte_rate = self.total_bytes / elapsed if elapsed > 0 else 0
        pct = self._pct()

        parts = [f"Progress: {self.txn_count:,} txns"]
        if pct:
            parts[0] += f" (~{pct:.1f}%)"

        parts.append(f"{self.obj_count:,} objects, {self.blob_count:,} blobs")
        parts.append(f"{_format_bytes(byte_rate)}/s, {txn_rate:.0f} txn/s")

        if pct > 0 and elapsed > 0:
            remaining = elapsed * (100 - pct) / pct
            parts.append(f"ETA: {_format_duration(remaining)}")

        log.info(" | ".join(parts))

    def log_summary(self, txn_count, obj_count, blob_count):
        """Log final summary."""
        elapsed = time.monotonic() - self.start_time
        txn_rate = txn_count / elapsed if elapsed > 0 else 0
        byte_rate = self.total_bytes / elapsed if elapsed > 0 else 0

        log.info(
            "Complete: %s transactions, %s objects, %s blobs, %s in %s (avg: %s txn/s, %s/s)",
            f"{txn_count:,}",
            f"{obj_count:,}",
            f"{blob_count:,}",
            _format_bytes(self.total_bytes),
            _format_duration(elapsed),
            f"{txn_rate:.0f}",
            _format_bytes(byte_rate),
        )
