"""CLI entry point for zodb-convert."""

from zodb_convert.config import open_storages
from zodb_convert.copier import copy_transactions
from zodb_convert.copier import get_incremental_start_tid
from zodb_convert.progress import ProgressReporter

import argparse
import contextlib
import logging
import sys


log = logging.getLogger("zodb-convert")


def parse_args(argv):
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        prog="zodb-convert",
        description="Copy ZODB data between any two compatible storages.",
    )

    parser.add_argument(
        "config_file",
        nargs="?",
        default=None,
        help="Traditional ZConfig configuration file with source/destination sections.",
    )

    source_group = parser.add_argument_group("source options")
    source_group.add_argument(
        "--source-zope-conf",
        metavar="FILE",
        help="Extract source storage from a zope.conf file.",
    )
    source_group.add_argument(
        "--source-db",
        default="main",
        metavar="NAME",
        help="Database name in source zope.conf (default: main).",
    )

    dest_group = parser.add_argument_group("destination options")
    dest_group.add_argument(
        "--dest-zope-conf",
        metavar="FILE",
        help="Extract destination storage from a zope.conf file.",
    )
    dest_group.add_argument(
        "--dest-db",
        default="main",
        metavar="NAME",
        help="Database name in destination zope.conf (default: main).",
    )

    behavior_group = parser.add_argument_group("behavior")
    behavior_group.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be copied without actually copying.",
    )
    behavior_group.add_argument(
        "--incremental",
        action="store_true",
        help="Resume from the last transaction in the destination.",
    )
    behavior_group.add_argument(
        "-w",
        "--workers",
        type=int,
        default=1,
        metavar="N",
        help=(
            "Number of parallel writer threads (default: 1). "
            "Only effective when the destination storage supports it "
            "(e.g. zodb-pgjsonb). Ignored for storages that don't."
        ),
    )
    behavior_group.add_argument(
        "--background-blobs",
        action="store_true",
        help=(
            "Upload blobs to S3 in a background thread pool, decoupled "
            "from PG writes. Faster for large migrations. Only effective "
            "with parallel workers (-w)."
        ),
    )
    behavior_group.add_argument(
        "--deferred-blobs",
        metavar="PATH",
        help=(
            "Write blob upload tasks to a manifest file instead of "
            "uploading to S3. Use --upload-blobs to process later. "
            "Only effective with parallel workers (-w)."
        ),
    )
    behavior_group.add_argument(
        "--upload-blobs",
        metavar="MANIFEST",
        help=(
            "Upload deferred blobs from a manifest file (created by "
            "--deferred-blobs). Requires a destination config for S3 "
            "credentials. Does not copy transactions."
        ),
    )
    behavior_group.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase verbosity (-v for INFO, -vv for DEBUG).",
    )

    args = parser.parse_args(argv)

    # Require at least one source/destination specification
    # (--upload-blobs only needs a destination, validated later)
    if (
        not args.config_file
        and not args.source_zope_conf
        and not args.dest_zope_conf
        and not args.upload_blobs
    ):
        parser.error(
            "At least one of config_file, --source-zope-conf, or --dest-zope-conf is required."
        )

    return args


def _setup_logging(verbose):
    """Configure logging based on verbosity level."""
    if verbose >= 2:
        level = logging.DEBUG
    elif verbose >= 1:
        level = logging.INFO
    else:
        level = logging.WARNING

    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))

    # Configure root logger so destination storage progress (e.g.
    # zodb_pgjsonb.storage) is visible during parallel delegation.
    root = logging.getLogger()
    root.setLevel(level)
    root.addHandler(handler)

    # Keep zodb-convert logger explicit for direct use.
    logging.getLogger("zodb-convert").setLevel(level)


def _open_destination(args):
    """Open only the destination storage from CLI args.

    Returns (destination_storage, closables).
    """
    from zodb_convert.config import open_storage_from_zope_conf
    from zodb_convert.config import open_storages_from_config

    destination = None
    closables = []

    if args.config_file:
        _cfg_source, cfg_dest = open_storages_from_config(args.config_file)
        if cfg_dest is not None:
            destination = cfg_dest
        # Close unused source if opened
        if _cfg_source is not None:
            closables.append(_cfg_source)

    if args.dest_zope_conf:
        if destination is not None:
            raise ValueError(
                "Destination specified in both config file and --dest-zope-conf"
            )
        db = open_storage_from_zope_conf(args.dest_zope_conf, args.dest_db)
        destination = db.storage
        closables.append(db)

    if destination is None:
        raise ValueError(
            "No destination storage configured. Use a config file or --dest-zope-conf."
        )

    return destination, closables


def main(argv=None):
    """Main entry point for zodb-convert."""
    args = parse_args(argv if argv is not None else sys.argv[1:])
    _setup_logging(args.verbose)

    if args.upload_blobs:
        from zodb_convert.manifest import upload_from_manifest

        closables = []
        try:
            destination, closables = _open_destination(args)
            s3_client = getattr(destination, "_s3_client", None)
            if s3_client is None:
                log.error("Destination storage has no S3 client configured")
                return 1
            stats = upload_from_manifest(
                args.upload_blobs,
                s3_client,
                workers=args.workers or 8,
            )
            if stats["failed"]:
                log.error("%d blob upload(s) failed", stats["failed"])
                return 1
            return 0
        except KeyboardInterrupt:
            log.warning("Interrupted by user, aborting...")
            return 130
        except (ValueError, FileNotFoundError) as e:
            log.error("%s", e)
            sys.exit(1)
        except Exception as e:
            log.error("Upload failed: %s", e, exc_info=True)
            sys.exit(2)
        finally:
            for obj in closables:
                with contextlib.suppress(Exception):
                    obj.close()

    closables = []
    try:
        source, destination, closables = open_storages(args)

        start_tid = None
        if args.incremental:
            start_tid = get_incremental_start_tid(source, destination)
            if start_tid is not None:
                log.info("Incremental mode: resuming from TID %r", start_tid)
            else:
                log.info("Incremental mode: destination is empty, full copy")

        blob_mode = "inline"
        if args.background_blobs:
            blob_mode = "background"
        elif args.deferred_blobs:
            blob_mode = f"deferred:{args.deferred_blobs}"

        if args.dry_run:
            log.info("Dry run mode: no data will be written")

        # len(source) returns OID count (O(1) for FileStorage) — used
        # for approximate progress percentage without iterating.
        total_oids = 0
        with contextlib.suppress(TypeError):
            total_oids = len(source)

        progress = ProgressReporter(
            total_oids=total_oids,
            verbose=args.verbose >= 1,
        )

        txn_count, obj_count, blob_count = copy_transactions(
            source,
            destination,
            start_tid=start_tid,
            dry_run=args.dry_run,
            progress=progress,
            workers=args.workers,
            blob_mode=blob_mode,
        )

        if txn_count is None:
            # Parallel delegation — destination already logged progress.
            log.info("Conversion complete (parallel delegation).")
        elif args.dry_run:
            progress.log_summary(txn_count, obj_count, blob_count)
            log.info("Dry run complete: %d transactions would be copied", txn_count)
        else:
            progress.log_summary(txn_count, obj_count, blob_count)
            log.info("Conversion complete: %d transactions copied", txn_count)

        return 0

    except KeyboardInterrupt:
        log.warning("Interrupted by user, aborting...")
        return 130
    except (ValueError, FileNotFoundError) as e:
        log.error("%s", e)
        sys.exit(1)
    except Exception as e:
        log.error("Conversion failed: %s", e, exc_info=True)
        sys.exit(2)
    finally:
        for obj in closables:
            with contextlib.suppress(Exception):
                obj.close()
