# Changelog

## 1.0.0b4

- Blob count now reflects actual blob file data transfers, not pickle class
  references. Replaces `is_blob_record()` heuristic with direct `loadBlob()`
  check per (oid, tid).

## 1.0.0b3

- Fix `--incremental` resume when destination has wall-clock TIDs from ZODB.DB
  initialization that overshoot the source TID range. The incremental logic now
  compares source/destination TID ranges and scans for the real resume point.

## 1.0.0b2

- Fix progress reporting: per-transaction record count was cumulative instead of per-transaction.

## 1.0.0b1

Initial release. Derived from
[RelStorage's zodbconvert](https://github.com/zodb/relstorage)
(Copyright Zope Foundation and Contributors, ZPL-2.1).

### Differences from RelStorage's zodbconvert

- **Standalone package**: no RelStorage dependency -- works with any
  ZODB-compatible storage (FileStorage, RelStorage, zodb-pgjsonb, ZEO, ...).
- **zope.conf extraction**: `--source-zope-conf` / `--dest-zope-conf` flags
  extract storage configuration from existing Zope config files, no need to
  write a separate conversion config.
- **Mixed mode**: combine a traditional config file with zope.conf extraction
  (e.g. destination from config file, source from zope.conf).
- **Enhanced progress reporting**: multi-tier output with per-transaction
  logging for small conversions, interval-based updates with throughput and
  ETA for large ones, and a summary at completion.
- **Incremental copy**: `--incremental` resumes from the last transaction in
  the destination storage.
- **Dry-run mode**: `--dry-run` previews what would be copied.
