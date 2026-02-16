"""CLI entry point for zodb-convert."""

import argparse
import logging
import sys

from zodb_convert.config import open_storages
from zodb_convert.copier import copy_transactions
from zodb_convert.copier import get_incremental_start_tid
from zodb_convert.progress import ProgressReporter


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
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase verbosity (-v for INFO, -vv for DEBUG).",
    )

    args = parser.parse_args(argv)

    # Require at least one source/destination specification
    if not args.config_file and not args.source_zope_conf and not args.dest_zope_conf:
        parser.error("At least one of config_file, --source-zope-conf, or --dest-zope-conf is required.")

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
    logger = logging.getLogger("zodb-convert")
    logger.setLevel(level)
    logger.addHandler(handler)


def main(argv=None):
    """Main entry point for zodb-convert."""
    args = parse_args(argv if argv is not None else sys.argv[1:])
    _setup_logging(args.verbose)

    closables = []
    try:
        source, destination, closables = open_storages(args)

        start_tid = None
        if args.incremental:
            start_tid = get_incremental_start_tid(destination)
            if start_tid is not None:
                log.info("Incremental mode: resuming from TID %r", start_tid)
            else:
                log.info("Incremental mode: destination is empty, full copy")

        if args.dry_run:
            log.info("Dry run mode: no data will be written")

        # Count total transactions for progress reporting
        total_txns = None
        try:
            it = source.iterator(start=start_tid)
            total_txns = 0
            for _txn in it:
                total_txns += 1
            if hasattr(it, "close"):
                it.close()
        except Exception:
            total_txns = None

        progress = ProgressReporter(
            total_txns=total_txns,
            verbose=args.verbose >= 1,
        )

        txn_count, obj_count, blob_count = copy_transactions(
            source,
            destination,
            start_tid=start_tid,
            dry_run=args.dry_run,
            progress=progress,
        )

        progress.log_summary(txn_count, obj_count, blob_count)

        if args.dry_run:
            log.info("Dry run complete: %d transactions would be copied", txn_count)
        else:
            log.info("Conversion complete: %d transactions copied", txn_count)

        return 0

    except (ValueError, FileNotFoundError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        log.error("Conversion failed: %s", e, exc_info=True)
        sys.exit(2)
    finally:
        for obj in closables:
            try:
                obj.close()
            except Exception:
                pass
