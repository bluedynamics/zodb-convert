"""Configuration handling: ZConfig schema + zope.conf extraction."""

import logging
import re

from io import StringIO

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


def open_storage_from_zope_conf(path, db_name="main"):
    """Extract storage configuration from a zope.conf file and return a ZODB.DB.

    Parses the zope.conf to find the <zodb_db db_name> section,
    converts it to a standalone <zodb> section, and opens via
    ZODB.config.databaseFromString().

    Returns a ZODB.DB object. Caller uses db.storage and must call db.close().
    """
    with open(path) as f:
        content = f.read()

    # Extract %import and %define directives
    directives = re.findall(r"^(%(?:import|define)\s+.*)$", content, re.MULTILINE)

    # Extract <zodb_db db_name>...</zodb_db> section
    pattern = r"(<zodb_db\s+" + re.escape(db_name) + r"\s*>.*?</zodb_db>)"
    match = re.search(pattern, content, re.DOTALL)
    if not match:
        raise ValueError(f"No <zodb_db {db_name}> section found in {path}")

    section = match.group(1)

    # Convert <zodb_db NAME> to <zodb NAME> format
    section = re.sub(r"<zodb_db\s+\S+\s*>", f"<zodb {db_name}>", section)
    section = section.replace("</zodb_db>", "</zodb>")

    # Remove Zope-specific keys
    for key in _ZOPE_SPECIFIC_KEYS:
        section = re.sub(rf"^\s*{key}\s+.*$", "", section, flags=re.MULTILINE)

    config_str = "\n".join(directives) + "\n" + section
    db = ZODB.config.databaseFromString(config_str)
    return db


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
            raise ValueError("Source specified in both config file and --source-zope-conf")
        db = open_storage_from_zope_conf(options.source_zope_conf, options.source_db)
        source = db.storage
        closables.append(db)

    # Destination from zope.conf
    if options.dest_zope_conf:
        if destination is not None:
            raise ValueError("Destination specified in both config file and --dest-zope-conf")
        db = open_storage_from_zope_conf(options.dest_zope_conf, options.dest_db)
        destination = db.storage
        closables.append(db)

    if source is None:
        raise ValueError("No source storage configured. Use a config file or --source-zope-conf.")
    if destination is None:
        raise ValueError("No destination storage configured. Use a config file or --dest-zope-conf.")

    return source, destination, closables
