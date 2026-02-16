# zodb-convert

Generic ZODB storage conversion tool. Copies data between any two ZODB-compatible storages.

Derived from [RelStorage's zodbconvert](https://github.com/zodb/relstorage) by the
Zope Foundation and Contributors, extracted as a standalone generic tool.

## Installation

```bash
pip install zodb-convert
```

To convert between specific storage types, install their packages too:

```bash
pip install zodb-convert ZODB RelStorage  # FileStorage ↔ RelStorage
pip install zodb-convert ZODB zodb-pgjsonb  # FileStorage ↔ PGJsonb
```

## Usage

### Traditional config file

Create a config file (`convert.conf`):

```
<source>
    <filestorage>
        path /data/Data.fs
        blob-dir /data/blobs
    </filestorage>
</source>

<destination>
    <filestorage>
        path /backup/Data.fs
        blob-dir /backup/blobs
    </filestorage>
</destination>
```

Run:

```bash
zodb-convert convert.conf
```

### Using existing zope.conf files

Extract storage configuration directly from Zope config files:

```bash
zodb-convert --source-zope-conf /old/etc/zope.conf --dest-zope-conf /new/etc/zope.conf
```

Specify which database to use (defaults to "main"):

```bash
zodb-convert --source-zope-conf zope.conf --source-db main \
             --dest-zope-conf other.conf --dest-db catalog
```

### Mixed mode

Combine traditional config with zope.conf extraction:

```bash
zodb-convert convert.conf --source-zope-conf /old/etc/zope.conf
```

Where `convert.conf` contains only the `<destination>` section.

### Options

- `--dry-run` — show what would be copied without making changes
- `--incremental` — resume from the last transaction in the destination
- `-v` / `--verbose` — increase verbosity (`-v` for INFO, `-vv` for DEBUG)

## License

ZPL-2.1 — see [LICENSE.txt](LICENSE.txt)
