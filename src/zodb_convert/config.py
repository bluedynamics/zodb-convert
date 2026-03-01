"""Configuration handling: ZConfig schema + zope.conf extraction."""

from io import StringIO

import logging
import re
import ZConfig
import ZODB.config


log = logging.getLogger("zodb-convert")

SCHEMA_XML = """\
<schema>
  <import package="ZODB"/>
  <section type="ZODB.storage" name="source" attribute="source" required="no" />
  <section type="ZODB.storage" name="destination" attribute="destination" required="no" />
</schema>
"""

# Keys that are Zope-specific and should be stripped from <zodb_db> sections
_ZOPE_SPECIFIC_KEYS = (
    "mount-point",
    "connection-class",
    "class-factory",
    "container-class",
)


def open_storages_from_config(config_path):
    """Open source and/or destination storages from a traditional ZConfig file.

    Args:
        config_path: Path to the ZConfig configuration file.

    Returns (source_storage_or_None, dest_storage_or_None).
    """
    schema = ZConfig.loadSchemaFile(StringIO(SCHEMA_XML))
    config, _ = ZConfig.loadConfig(schema, config_path)
    source = config.source.open() if config.source else None
    destination = config.destination.open() if config.destination else None
    return source, destination


def _extract_zodb_db_section(path, db_name):
    """Extract the <zodb_db db_name> section from a zope.conf file.

    Returns (directives_list, section_text).
    """
    with open(path) as f:
        content = f.read()

    directives = re.findall(r"^(%(?:import|define)\s+.*)$", content, re.MULTILINE)

    pattern = r"(<zodb_db\s+" + re.escape(db_name) + r"\s*>.*?</zodb_db>)"
    match = re.search(pattern, content, re.DOTALL)
    if not match:
        raise ValueError(f"No <zodb_db {db_name}> section found in {path}")

    section = match.group(1)

    # Remove Zope-specific keys
    for key in _ZOPE_SPECIFIC_KEYS:
        section = re.sub(rf"^\s*{key}\s+.*$", "", section, flags=re.MULTILINE)

    return directives, section


def _extract_inner_storage(section):
    """Extract the outermost storage section from a <zodb_db> block.

    Finds the first non-wrapper opening tag and its matching closing tag,
    correctly handling nested sections like <z3blobs><pgjsonb>...</pgjsonb></z3blobs>.
    """
    # Find the first opening tag that isn't the zodb_db/zodb wrapper
    start_match = re.search(r"<(?!zodb_db\b|zodb\b|/)(\w[\w-]*)", section)
    if not start_match:
        return None
    tag_name = start_match.group(1)
    start_pos = start_match.start()
    # Find the last matching closing tag (inner nested ones close first)
    close_pattern = rf"</{re.escape(tag_name)}\s*>"
    close_matches = list(re.finditer(close_pattern, section))
    if not close_matches:
        return None
    end_pos = close_matches[-1].end()
    return section[start_pos:end_pos]


def open_storage_from_zope_conf(path, db_name="main"):
    """Extract storage from a zope.conf file and open it directly.

    Opens the storage without wrapping in ZODB.DB, avoiding automatic
    root object creation which would pollute TIDs for conversion.

    Returns the storage object. Caller must call storage.close().
    """
    directives, section = _extract_zodb_db_section(path, db_name)

    inner = _extract_inner_storage(section)
    if inner:
        config_str = "\n".join(directives) + "\n" + inner
        return ZODB.config.storageFromString(config_str)

    # Fallback: wrap in <zodb> and open via DB (shouldn't normally happen)
    section = re.sub(r"<zodb_db\s+\S+\s*>", f"<zodb {db_name}>", section)
    section = section.replace("</zodb_db>", "</zodb>")
    config_str = "\n".join(directives) + "\n" + section
    db = ZODB.config.databaseFromString(config_str)
    storage = db.storage
    db.close()
    return storage


def open_storages(options):
    """Open source and destination storages from CLI options.

    Supports three modes:
    1. Traditional config file (both source and dest)
    2. Both from zope.conf files
    3. Mixed (one from zope.conf, other from config file)

    Returns (source_storage, dest_storage, closables) where closables
    is a list of objects to close when done (DB objects from zope.conf).
    """
    source = None
    destination = None
    closables = []

    # From traditional config file
    if options.config_file:
        cfg_source, cfg_dest = open_storages_from_config(options.config_file)
        if cfg_source is not None:
            source = cfg_source
        if cfg_dest is not None:
            destination = cfg_dest

    # Source from zope.conf
    if options.source_zope_conf:
        if source is not None:
            raise ValueError(
                "Source specified in both config file and --source-zope-conf"
            )
        source = open_storage_from_zope_conf(
            options.source_zope_conf, options.source_db
        )
        closables.append(source)

    # Destination from zope.conf
    if options.dest_zope_conf:
        if destination is not None:
            raise ValueError(
                "Destination specified in both config file and --dest-zope-conf"
            )
        destination = open_storage_from_zope_conf(
            options.dest_zope_conf, options.dest_db
        )
        closables.append(destination)

    if source is None:
        raise ValueError(
            "No source storage configured. Use a config file or --source-zope-conf."
        )
    if destination is None:
        raise ValueError(
            "No destination storage configured. Use a config file or --dest-zope-conf."
        )

    return source, destination, closables
