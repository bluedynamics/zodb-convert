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
<filestorage source>
    path /data/Data.fs
    blob-dir /data/blobs
</filestorage>

<filestorage destination>
    path /backup/Data.fs
    blob-dir /backup/blobs
</filestorage>
```

For storages that need blob wrapping:

```
<blobstorage source>
    blob-dir /data/blobs
    <filestorage>
        path /data/Data.fs
    </filestorage>
</blobstorage>

<filestorage destination>
    path /backup/Data.fs
    blob-dir /backup/blobs
</filestorage>
```

For third-party storages, use `%import`:

```
%import relstorage

<relstorage source>
    <postgresql>
        dsn dbname=zodb user=zodb
    </postgresql>
</relstorage>

<filestorage destination>
    path /backup/Data.fs
</filestorage>
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

Where `convert.conf` contains only the destination storage section.

### Options

- `--dry-run` — show what would be copied without making changes
- `--incremental` — resume from the last transaction in the destination
- `-v` / `--verbose` — increase verbosity (`-v` for INFO, `-vv` for DEBUG)

## Source Code and Contributions

The source code is managed in a Git repository, with its main branches hosted on GitHub.
Issues can be reported there too.

We'd be happy to see many forks and pull requests to make this package even better.
We welcome AI-assisted contributions, but expect every contributor to fully understand and be able to explain the code they submit.
Please don't send bulk auto-generated pull requests.

Maintainers are Jens Klein and the BlueDynamics Alliance developer team.
We appreciate any contribution and if a release on PyPI is needed, please just contact one of us.
We also offer commercial support if any training, coaching, integration or adaptations are needed.

## License

ZPL-2.1 — see [LICENSE.txt](https://github.com/bluedynamics/zodb-convert/blob/main/LICENSE.txt)
