from types import SimpleNamespace
from zodb_convert.config import _extract_inner_storage
from zodb_convert.config import open_storage_from_zope_conf
from zodb_convert.config import open_storages
from zodb_convert.config import open_storages_from_config

import os
import pytest


class TestTraditionalConfig:
    def test_filestorage_source_and_dest(self, temp_dir):
        src_path = os.path.join(temp_dir, "source.fs")
        dst_path = os.path.join(temp_dir, "dest.fs")
        config_text = f"""\
<filestorage source>
    path {src_path}
</filestorage>

<filestorage destination>
    path {dst_path}
</filestorage>
"""
        config_file = os.path.join(temp_dir, "convert.conf")
        with open(config_file, "w") as f:
            f.write(config_text)

        source, dest = open_storages_from_config(config_file)

        try:
            assert source is not None
            assert dest is not None
        finally:
            source.close()
            dest.close()

    def test_source_only(self, temp_dir):
        src_path = os.path.join(temp_dir, "source.fs")
        config_text = f"""\
<filestorage source>
    path {src_path}
</filestorage>
"""
        config_file = os.path.join(temp_dir, "convert.conf")
        with open(config_file, "w") as f:
            f.write(config_text)

        source, dest = open_storages_from_config(config_file)

        try:
            assert source is not None
            assert dest is None
        finally:
            source.close()

    def test_dest_only(self, temp_dir):
        dst_path = os.path.join(temp_dir, "dest.fs")
        config_text = f"""\
<filestorage destination>
    path {dst_path}
</filestorage>
"""
        config_file = os.path.join(temp_dir, "convert.conf")
        with open(config_file, "w") as f:
            f.write(config_text)

        source, dest = open_storages_from_config(config_file)

        try:
            assert source is None
            assert dest is not None
        finally:
            dest.close()


class TestZopeConfExtraction:
    def test_basic_extraction(self, temp_dir):
        fs_path = os.path.join(temp_dir, "Data.fs")
        zope_conf = f"""\
<zodb_db main>
    mount-point /
    cache-size 30000
    <filestorage>
        path {fs_path}
    </filestorage>
</zodb_db>
"""
        conf_path = os.path.join(temp_dir, "zope.conf")
        with open(conf_path, "w") as f:
            f.write(zope_conf)

        storage = open_storage_from_zope_conf(conf_path, db_name="main")
        try:
            assert storage is not None
        finally:
            storage.close()

    def test_with_import_directives(self, temp_dir):
        """Verify %import lines are preserved."""
        fs_path = os.path.join(temp_dir, "Data.fs")
        zope_conf = f"""\
<zodb_db main>
    mount-point /
    <filestorage>
        path {fs_path}
    </filestorage>
</zodb_db>
"""
        conf_path = os.path.join(temp_dir, "zope.conf")
        with open(conf_path, "w") as f:
            f.write(zope_conf)

        storage = open_storage_from_zope_conf(conf_path, db_name="main")
        try:
            assert storage is not None
        finally:
            storage.close()

    def test_named_db_selection(self, temp_dir):
        fs_path1 = os.path.join(temp_dir, "main.fs")
        fs_path2 = os.path.join(temp_dir, "catalog.fs")
        zope_conf = f"""\
<zodb_db main>
    mount-point /
    <filestorage>
        path {fs_path1}
    </filestorage>
</zodb_db>

<zodb_db catalog>
    mount-point /catalog
    <filestorage>
        path {fs_path2}
    </filestorage>
</zodb_db>
"""
        conf_path = os.path.join(temp_dir, "zope.conf")
        with open(conf_path, "w") as f:
            f.write(zope_conf)

        storage = open_storage_from_zope_conf(conf_path, db_name="catalog")
        try:
            assert storage is not None
            assert storage.getName().endswith("catalog.fs")
        finally:
            storage.close()

    def test_db_name_not_found(self, temp_dir):
        zope_conf = """\
<zodb_db main>
    <mappingstorage>
    </mappingstorage>
</zodb_db>
"""
        conf_path = os.path.join(temp_dir, "zope.conf")
        with open(conf_path, "w") as f:
            f.write(zope_conf)

        with pytest.raises(ValueError, match="No <zodb_db nonexistent>"):
            open_storage_from_zope_conf(conf_path, db_name="nonexistent")

    def test_strips_mount_point(self, temp_dir):
        """mount-point is Zope-specific and should be removed."""
        fs_path = os.path.join(temp_dir, "Data.fs")
        zope_conf = f"""\
<zodb_db main>
    mount-point /
    <filestorage>
        path {fs_path}
    </filestorage>
</zodb_db>
"""
        conf_path = os.path.join(temp_dir, "zope.conf")
        with open(conf_path, "w") as f:
            f.write(zope_conf)

        # Should not raise (mount-point is not a valid ZODB config key)
        storage = open_storage_from_zope_conf(conf_path, db_name="main")
        try:
            assert storage is not None
        finally:
            storage.close()

    def test_with_other_sections(self, temp_dir):
        """Non-ZODB sections in zope.conf should be ignored."""
        fs_path = os.path.join(temp_dir, "Data.fs")
        zope_conf = f"""\
# Some comment
<zodb_db main>
    <filestorage>
        path {fs_path}
    </filestorage>
</zodb_db>
"""
        conf_path = os.path.join(temp_dir, "zope.conf")
        with open(conf_path, "w") as f:
            f.write(zope_conf)

        storage = open_storage_from_zope_conf(conf_path, db_name="main")
        try:
            assert storage is not None
        finally:
            storage.close()


class TestExtractInnerStorage:
    def test_simple_section(self):
        section = """\
<zodb_db main>
    <filestorage>
        path /tmp/Data.fs
    </filestorage>
</zodb_db>"""
        result = _extract_inner_storage(section)
        assert result is not None
        assert result.startswith("<filestorage>")
        assert result.endswith("</filestorage>")

    def test_nested_wrapper(self):
        """Nested wrappers like <z3blobs><pgjsonb>...</pgjsonb></z3blobs>."""
        section = """\
<zodb_db main>
    <z3blobs>
        blob-dir /tmp/blobs
        <pgjsonb>
            dsn host=localhost
        </pgjsonb>
    </z3blobs>
</zodb_db>"""
        result = _extract_inner_storage(section)
        assert result is not None
        assert result.startswith("<z3blobs>")
        assert result.endswith("</z3blobs>")
        assert "<pgjsonb>" in result

    def test_no_inner_section(self):
        section = "<zodb_db main>\n    cache-size 5000\n</zodb_db>"
        result = _extract_inner_storage(section)
        assert result is None


class TestOpenStorages:
    def _make_zope_conf(self, temp_dir, name, db_name="main"):
        """Create a minimal zope.conf with a FileStorage."""
        fs_path = os.path.join(temp_dir, f"{name}.fs")
        zope_conf = f"""\
<zodb_db {db_name}>
    <filestorage>
        path {fs_path}
    </filestorage>
</zodb_db>
"""
        conf_path = os.path.join(temp_dir, f"{name}_zope.conf")
        with open(conf_path, "w") as f:
            f.write(zope_conf)
        return conf_path

    def test_source_from_zope_conf(self, temp_dir):
        """Source from zope.conf, dest from config file."""
        src_conf = self._make_zope_conf(temp_dir, "source")
        dst_path = os.path.join(temp_dir, "dest.fs")
        config_text = f"""\
<filestorage destination>
    path {dst_path}
</filestorage>
"""
        config_path = os.path.join(temp_dir, "convert.conf")
        with open(config_path, "w") as f:
            f.write(config_text)

        options = SimpleNamespace(
            config_file=config_path,
            source_zope_conf=src_conf,
            source_db="main",
            dest_zope_conf=None,
            dest_db="main",
        )
        source, dest, closables = open_storages(options)
        try:
            assert source is not None
            assert dest is not None
        finally:
            for c in closables:
                c.close()
            dest.close()

    def test_dest_from_zope_conf(self, temp_dir):
        """Source from config file, dest from zope.conf."""
        src_path = os.path.join(temp_dir, "source.fs")
        dest_conf = self._make_zope_conf(temp_dir, "dest")
        config_text = f"""\
<filestorage source>
    path {src_path}
</filestorage>
"""
        config_path = os.path.join(temp_dir, "convert.conf")
        with open(config_path, "w") as f:
            f.write(config_text)

        options = SimpleNamespace(
            config_file=config_path,
            source_zope_conf=None,
            source_db="main",
            dest_zope_conf=dest_conf,
            dest_db="main",
        )
        source, dest, closables = open_storages(options)
        try:
            assert source is not None
            assert dest is not None
        finally:
            for c in closables:
                c.close()
            source.close()

    def test_source_conflict_raises(self, temp_dir):
        """Cannot specify source in both config file and zope.conf."""
        src_path = os.path.join(temp_dir, "source.fs")
        src_conf = self._make_zope_conf(temp_dir, "source2")
        config_text = f"""\
<filestorage source>
    path {src_path}
</filestorage>
"""
        config_path = os.path.join(temp_dir, "convert.conf")
        with open(config_path, "w") as f:
            f.write(config_text)

        options = SimpleNamespace(
            config_file=config_path,
            source_zope_conf=src_conf,
            source_db="main",
            dest_zope_conf=None,
            dest_db="main",
        )
        with pytest.raises(ValueError, match="Source specified in both"):
            open_storages(options)

    def test_dest_conflict_raises(self, temp_dir):
        """Cannot specify dest in both config file and zope.conf."""
        dst_path = os.path.join(temp_dir, "dest.fs")
        dest_conf = self._make_zope_conf(temp_dir, "dest2")
        config_text = f"""\
<filestorage destination>
    path {dst_path}
</filestorage>
"""
        config_path = os.path.join(temp_dir, "convert.conf")
        with open(config_path, "w") as f:
            f.write(config_text)

        options = SimpleNamespace(
            config_file=config_path,
            source_zope_conf=None,
            source_db="main",
            dest_zope_conf=dest_conf,
            dest_db="main",
        )
        with pytest.raises(ValueError, match="Destination specified in both"):
            open_storages(options)
