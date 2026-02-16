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
    """Multi-tier progress reporter for ZODB storage conversion.

    Tier 1: Per-transaction logging for small conversions or verbose mode.
    Tier 2: Interval-based logging (every 10s or 100 txns).
    Tier 3: Summary at completion.
    """

    def __init__(self, total_txns=None, verbose=False, log_interval=10, log_count=100):
        self.total_txns = total_txns
        self.verbose = verbose
        self.log_interval = log_interval
        self.log_count = log_count

        self.txn_count = 0
        self.obj_count = 0
        self.blob_count = 0
        self.total_bytes = 0

        self.start_time = time.monotonic()
        self.last_log_time = self.start_time
        self.last_log_txn_count = 0

    @property
    def _per_transaction(self):
        """Whether to log every transaction."""
        if self.verbose:
            return True
        return self.total_txns is not None and self.total_txns < 100

    def on_transaction(self, tid, record_count, byte_size, blob_count):
        """Called after each transaction is copied."""
        self.txn_count += 1
        self.obj_count += record_count
        self.blob_count += blob_count
        self.total_bytes += byte_size

        now = time.monotonic()
        is_first = self.txn_count == 1

        if self._per_transaction or is_first:
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
        pct = ""
        if self.total_txns:
            pct = f" ({self.txn_count * 100 / self.total_txns:.0f}%)"
        total = f"/{self.total_txns}" if self.total_txns else ""
        log.info(
            "TX %s%s%s tid=%s %d records, %d blobs, %s",
            self.txn_count,
            total,
            pct,
            readable_tid_repr(tid),
            record_count,
            blob_count,
            _format_bytes(byte_size),
        )

    def _log_interval(self, now):
        elapsed = now - self.start_time
        txn_rate = self.txn_count / elapsed if elapsed > 0 else 0
        byte_rate = self.total_bytes / elapsed if elapsed > 0 else 0

        parts = [f"Progress: {self.txn_count:,}"]
        if self.total_txns:
            pct = self.txn_count * 100 / self.total_txns
            parts[0] += f"/{self.total_txns:,} txns ({pct:.1f}%)"
        else:
            parts[0] += " txns"

        parts.append(f"{self.obj_count:,} objects, {self.blob_count:,} blobs")
        parts.append(f"{_format_bytes(byte_rate)}/s, {txn_rate:.0f} txn/s")

        if self.total_txns and txn_rate > 0:
            remaining = (self.total_txns - self.txn_count) / txn_rate
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
