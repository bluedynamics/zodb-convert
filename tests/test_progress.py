from ZODB.utils import p64
from zodb_convert.progress import _format_bytes
from zodb_convert.progress import _format_duration
from zodb_convert.progress import ProgressReporter

import logging


class TestFormatHelpers:
    def test_format_bytes_small(self):
        assert _format_bytes(500) == "500 B"

    def test_format_bytes_kb(self):
        assert _format_bytes(2048) == "2.0 KB"

    def test_format_bytes_mb(self):
        assert _format_bytes(5 * 1024 * 1024) == "5.0 MB"

    def test_format_bytes_gb(self):
        assert _format_bytes(2 * 1024 * 1024 * 1024) == "2.0 GB"

    def test_format_duration_seconds(self):
        assert _format_duration(30.5) == "30.5s"

    def test_format_duration_minutes(self):
        assert _format_duration(150) == "2m 30s"

    def test_format_duration_hours(self):
        assert _format_duration(3700) == "1h 1m"


class TestProgressReporter:
    def test_initial_state(self):
        p = ProgressReporter()
        assert p.txn_count == 0
        assert p.obj_count == 0
        assert p.blob_count == 0
        assert p.total_bytes == 0

    def test_on_transaction_updates_counters(self):
        p = ProgressReporter(total_oids=10)
        tid = p64(1)
        p.on_transaction(tid, record_count=5, byte_size=1024, blob_count=1)
        assert p.txn_count == 1
        assert p.obj_count == 5
        assert p.blob_count == 1
        assert p.total_bytes == 1024

    def test_verbose_mode_logs_every_transaction(self, caplog):
        p = ProgressReporter(total_oids=1000, verbose=True)
        with caplog.at_level(logging.INFO, logger="zodb-convert"):
            for i in range(5):
                p.on_transaction(
                    p64(i + 1), record_count=1, byte_size=100, blob_count=0
                )
        # In verbose mode, every transaction should be logged
        assert len(caplog.records) == 5

    def test_verbose_logs_without_total(self, caplog):
        """Verbose logging works even without total_oids."""
        p = ProgressReporter(verbose=True)
        with caplog.at_level(logging.INFO, logger="zodb-convert"):
            for i in range(3):
                p.on_transaction(
                    p64(i + 1), record_count=1, byte_size=100, blob_count=0
                )
        assert len(caplog.records) == 3

    def test_large_conversion_interval_logging(self, caplog):
        p = ProgressReporter(total_oids=1000, log_interval=0.001, log_count=3)
        with caplog.at_level(logging.INFO, logger="zodb-convert"):
            for i in range(10):
                p.on_transaction(
                    p64(i + 1),
                    record_count=1,
                    byte_size=100,
                    blob_count=0,
                    oids=[p64(i + 1)],
                )
        # First transaction always logged, then interval-based
        assert len(caplog.records) >= 2

    def test_log_summary(self, caplog):
        p = ProgressReporter(total_oids=100)
        p.total_bytes = 1024 * 1024
        with caplog.at_level(logging.INFO, logger="zodb-convert"):
            p.log_summary(txn_count=100, obj_count=500, blob_count=10)
        assert len(caplog.records) == 1
        assert "Complete:" in caplog.records[0].message
        assert "100" in caplog.records[0].message
        assert "500" in caplog.records[0].message

    def test_interval_logging_without_total(self, caplog):
        """Without total_oids and not verbose, should use interval logging."""
        p = ProgressReporter(verbose=False, log_count=5)
        with caplog.at_level(logging.INFO, logger="zodb-convert"):
            for i in range(3):
                p.on_transaction(
                    p64(i + 1), record_count=1, byte_size=100, blob_count=0
                )
        # First transaction always logged
        assert len(caplog.records) >= 1

    def test_oid_tracking_for_progress(self):
        """OIDs passed via on_transaction are tracked for progress %."""
        p = ProgressReporter(total_oids=10)
        oid1 = p64(1)
        oid2 = p64(2)
        p.on_transaction(
            p64(1), record_count=2, byte_size=100, blob_count=0, oids=[oid1, oid2]
        )
        assert len(p._seen_oids) == 2
        assert p._pct() == 20.0  # 2/10

    def test_oid_dedup(self):
        """Same OID in multiple transactions is counted once."""
        p = ProgressReporter(total_oids=10)
        oid1 = p64(1)
        p.on_transaction(
            p64(1), record_count=1, byte_size=100, blob_count=0, oids=[oid1]
        )
        p.on_transaction(
            p64(2), record_count=1, byte_size=100, blob_count=0, oids=[oid1]
        )
        assert len(p._seen_oids) == 1
        assert p._pct() == 10.0  # 1/10

    def test_ema_initializes_on_first_sample(self):
        """EMA rate is seeded from the first measurement."""
        p = ProgressReporter(total_oids=100)
        assert p._ema_rate == 0.0
        # Simulate time passing and OIDs arriving
        p._seen_oids.update(range(10))
        p._last_ema_time = p.start_time  # ensure dt > MIN_INTERVAL
        p._update_ema(p.start_time + 2.0)
        assert p._ema_rate > 0  # seeded, not zero

    def test_ema_smooths_rate_changes(self):
        """EMA blends new samples instead of jumping."""
        p = ProgressReporter(total_oids=1000)
        # First sample: 100 oids in 1s = 100/s
        p._seen_oids.update(range(100))
        p._update_ema(p.start_time + 1.0)
        rate_after_first = p._ema_rate
        assert rate_after_first == 100.0  # first sample seeds directly

        # Second sample: 10 oids in 1s = 10/s — should NOT jump to 10
        p._seen_oids.update(range(100, 110))
        p._update_ema(p.start_time + 2.0)
        assert p._ema_rate > 10  # smoothed, not instant
        assert p._ema_rate < 100  # moved toward new rate

    def test_eta_empty_without_oids(self):
        """No ETA when total_oids is 0."""
        p = ProgressReporter(total_oids=0)
        assert p._eta() == ""

    def test_eta_returns_string_with_data(self):
        """ETA available after EMA has data."""
        p = ProgressReporter(total_oids=100)
        p._seen_oids.update(range(50))
        p._update_ema(p.start_time + 1.0)
        eta = p._eta()
        assert eta.startswith(", ETA:")
