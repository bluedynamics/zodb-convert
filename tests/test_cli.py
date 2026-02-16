"""Tests for CLI entry point."""

import os

import pytest

from zodb_convert.cli import main
from zodb_convert.cli import parse_args


class TestParseArgs:
    def test_config_file_only(self):
        args = parse_args(["convert.conf"])
        assert args.config_file == "convert.conf"
        assert args.source_zope_conf is None
        assert args.dest_zope_conf is None

    def test_source_zope_conf(self):
        args = parse_args(["--source-zope-conf", "old.conf"])
        assert args.source_zope_conf == "old.conf"
        assert args.source_db == "main"

    def test_dest_zope_conf(self):
        args = parse_args(["--dest-zope-conf", "new.conf"])
        assert args.dest_zope_conf == "new.conf"
        assert args.dest_db == "main"

    def test_both_zope_conf(self):
        args = parse_args([
            "--source-zope-conf", "old.conf",
            "--dest-zope-conf", "new.conf",
        ])
        assert args.source_zope_conf == "old.conf"
        assert args.dest_zope_conf == "new.conf"

    def test_mixed_mode(self):
        args = parse_args([
            "convert.conf",
            "--source-zope-conf", "old.conf",
        ])
        assert args.config_file == "convert.conf"
        assert args.source_zope_conf == "old.conf"

    def test_custom_db_names(self):
        args = parse_args([
            "--source-zope-conf", "old.conf",
            "--source-db", "catalog",
            "--dest-zope-conf", "new.conf",
            "--dest-db", "catalog",
        ])
        assert args.source_db == "catalog"
        assert args.dest_db == "catalog"

    def test_dry_run_flag(self):
        args = parse_args(["convert.conf", "--dry-run"])
        assert args.dry_run is True

    def test_incremental_flag(self):
        args = parse_args(["convert.conf", "--incremental"])
        assert args.incremental is True

    def test_verbose_default(self):
        args = parse_args(["convert.conf"])
        assert args.verbose == 0

    def test_verbose_single(self):
        args = parse_args(["convert.conf", "-v"])
        assert args.verbose == 1

    def test_verbose_double(self):
        args = parse_args(["convert.conf", "-vv"])
        assert args.verbose == 2

    def test_no_args_prints_help(self):
        """No args should show help and exit with error."""
        with pytest.raises(SystemExit) as exc_info:
            parse_args([])
        assert exc_info.value.code != 0


class TestMainValidation:
    def test_no_source_configured(self, temp_dir):
        """If only a config file with destination is provided, source is missing."""
        dst_path = os.path.join(temp_dir, "dest.fs")
        config = f"""\
<filestorage destination>
    path {dst_path}
</filestorage>
"""
        config_path = os.path.join(temp_dir, "convert.conf")
        with open(config_path, "w") as f:
            f.write(config)

        with pytest.raises(SystemExit) as exc_info:
            main([config_path])
        assert exc_info.value.code != 0

    def test_no_dest_configured(self, temp_dir):
        """If only a config file with source is provided, destination is missing."""
        src_path = os.path.join(temp_dir, "source.fs")
        config = f"""\
<filestorage source>
    path {src_path}
</filestorage>
"""
        config_path = os.path.join(temp_dir, "convert.conf")
        with open(config_path, "w") as f:
            f.write(config)

        with pytest.raises(SystemExit) as exc_info:
            main([config_path])
        assert exc_info.value.code != 0


def _write_config(temp_dir, src_path, dst_path, src_blob_dir=None, dst_blob_dir=None):
    """Write a ZConfig config file and return its path."""
    src_section = f"<filestorage source>\n    path {src_path}\n"
    if src_blob_dir:
        src_section += f"    blob-dir {src_blob_dir}\n"
    src_section += "</filestorage>"

    dst_section = f"<filestorage destination>\n    path {dst_path}\n"
    if dst_blob_dir:
        dst_section += f"    blob-dir {dst_blob_dir}\n"
    dst_section += "</filestorage>"

    config_text = f"{src_section}\n\n{dst_section}\n"
    config_path = os.path.join(temp_dir, "convert.conf")
    with open(config_path, "w") as f:
        f.write(config_text)
    return config_path


def _create_source(src_path, data, note=None):
    """Create a FileStorage with data and return it closed."""
    import transaction

    import ZODB
    import ZODB.FileStorage

    src_storage = ZODB.FileStorage.FileStorage(src_path)
    db = ZODB.DB(src_storage)
    conn = db.open()
    root = conn.root()
    root.update(data)
    if note:
        transaction.get().note(note)
    transaction.commit()
    conn.close()
    db.close()


class TestMainEndToEnd:
    def test_full_copy_via_config_file(self, temp_dir):
        """End-to-end: copy from one FileStorage to another using a config file."""
        src_path = os.path.join(temp_dir, "source.fs")
        dst_path = os.path.join(temp_dir, "dest.fs")

        _create_source(src_path, {"key": "value"}, note="test txn")
        config_path = _write_config(temp_dir, src_path, dst_path)

        result = main([config_path])
        assert result == 0

        # Verify destination has the data
        import ZODB
        import ZODB.FileStorage

        dst_storage = ZODB.FileStorage.FileStorage(dst_path, read_only=True)
        dst_db = ZODB.DB(dst_storage)
        dst_conn = dst_db.open()
        dst_root = dst_conn.root()
        assert dst_root["key"] == "value"
        dst_conn.close()
        dst_db.close()

    def test_dry_run_no_writes(self, temp_dir):
        """Dry run should not create the destination file."""
        src_path = os.path.join(temp_dir, "source.fs")
        dst_path = os.path.join(temp_dir, "dest.fs")

        _create_source(src_path, {"key": "value"})
        config_path = _write_config(temp_dir, src_path, dst_path)

        result = main([config_path, "--dry-run"])
        assert result == 0

    def test_incremental_copy(self, temp_dir):
        """Incremental copy should only copy new transactions."""
        import transaction

        import ZODB
        import ZODB.FileStorage

        src_path = os.path.join(temp_dir, "source.fs")
        dst_path = os.path.join(temp_dir, "dest.fs")

        _create_source(src_path, {"key1": "value1"}, note="txn1")
        config_path = _write_config(temp_dir, src_path, dst_path)

        # First copy
        result = main([config_path])
        assert result == 0

        # Add more data to source
        src_storage2 = ZODB.FileStorage.FileStorage(src_path)
        db2 = ZODB.DB(src_storage2)
        conn2 = db2.open()
        root2 = conn2.root()
        root2["key2"] = "value2"
        transaction.get().note("txn2")
        transaction.commit()
        conn2.close()
        db2.close()

        # Incremental copy
        result = main([config_path, "--incremental"])
        assert result == 0

        # Verify both values present in destination
        dst_storage = ZODB.FileStorage.FileStorage(dst_path, read_only=True)
        dst_db = ZODB.DB(dst_storage)
        dst_conn = dst_db.open()
        dst_root = dst_conn.root()
        assert dst_root["key1"] == "value1"
        assert dst_root["key2"] == "value2"
        dst_conn.close()
        dst_db.close()

    def test_verbose_output(self, temp_dir):
        """Verbose mode should work without error."""
        src_path = os.path.join(temp_dir, "source.fs")
        dst_path = os.path.join(temp_dir, "dest.fs")

        _create_source(src_path, {"key": "value"})
        config_path = _write_config(temp_dir, src_path, dst_path)

        result = main([config_path, "-v"])
        assert result == 0
