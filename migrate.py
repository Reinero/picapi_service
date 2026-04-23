from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from config import METADATA_SQLITE_PATH, MIGRATION_BATCH_SIZE, MIGRATION_CHECKPOINT_PATH, PG_DSN, PG_POOL_MAX, PG_POOL_MIN
from services.metadata_migration import migrate_sqlite_to_pg, reset_migration_checkpoint


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate metadata rows from SQLite to PostgreSQL.")
    parser.add_argument("--dry-run", action="store_true", help="Scan and report only, do not write to PostgreSQL.")
    parser.add_argument("--limit", type=int, default=0, help="Only migrate first N rows (0 means no limit).")
    parser.add_argument("--batch-size", type=int, default=MIGRATION_BATCH_SIZE, help="Rows per batch.")
    parser.add_argument("--resume", action="store_true", default=True, help="Resume from checkpoint (default true).")
    parser.add_argument("--no-resume", action="store_true", help="Ignore checkpoint and start from beginning.")
    parser.add_argument("--reset-checkpoint", action="store_true", help="Delete checkpoint before migration.")
    parser.add_argument(
        "--checkpoint-path",
        default=str(MIGRATION_CHECKPOINT_PATH),
        help="Checkpoint JSON file path.",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    args = parse_args()

    if args.reset_checkpoint:
        reset_migration_checkpoint(args.checkpoint_path)

    resume = False if args.no_resume else bool(args.resume)
    if not PG_DSN:
        raise RuntimeError("PG_DSN is required for migrate.py")

    metadata_app_root = Path(__file__).resolve().parent / "app"
    if str(metadata_app_root) not in sys.path:
        sys.path.insert(0, str(metadata_app_root))

    from metadata.pg_store import PostgresMetadataStore  # lazy import
    from metadata.sqlite_store import SQLiteMetadataStore

    sqlite_store = SQLiteMetadataStore(METADATA_SQLITE_PATH)
    sqlite_store.init()
    pg_store = PostgresMetadataStore(PG_DSN, minconn=PG_POOL_MIN, maxconn=PG_POOL_MAX)
    pg_store.init()

    stats = migrate_sqlite_to_pg(
        sqlite_store,
        pg_store,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
        limit=args.limit,
        resume=resume,
        checkpoint_path=args.checkpoint_path,
    )
    print(stats)


if __name__ == "__main__":
    main()
