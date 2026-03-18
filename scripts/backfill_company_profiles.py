#!/usr/bin/env python3
"""Backfill normalized A-share company profiles into SQLite."""

from __future__ import annotations

import argparse
import logging

from neocortex.log import configure_logging
from neocortex.storage import (
    DEFAULT_DB_PATH,
    backfill_company_profiles,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="backfill_company_profiles")
    parser.add_argument(
        "--db-path",
        default=str(DEFAULT_DB_PATH),
    )
    parser.add_argument("--timeout", type=float, default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--retry-count", type=int, default=0)
    parser.add_argument("--sleep-seconds", type=float, default=0.0)
    parser.add_argument("--workers", type=int, default=8)
    return parser


def main() -> int:
    configure_logging(logging.INFO, format="%(levelname)s %(message)s")
    args = build_parser().parse_args()
    stats = backfill_company_profiles(
        args.db_path,
        timeout=args.timeout,
        limit=args.limit,
        retry_count=args.retry_count,
        sleep_seconds=args.sleep_seconds,
        workers=args.workers,
    )
    logging.info("Using SQLite database at %s", args.db_path)
    logging.info(
        "Backfill complete: processed=%s fetched=%s skipped_unsupported=%s failed=%s",
        stats.processed,
        stats.fetched,
        stats.skipped_unsupported,
        stats.failed,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
