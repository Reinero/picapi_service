from __future__ import annotations

import json
import logging
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any

from config import (
    METADATA_SQLITE_PATH,
    MIGRATION_AUTO_RUN,
    MIGRATION_BATCH_SIZE,
    MIGRATION_CHECKPOINT_PATH,
    MIGRATION_DRY_RUN,
    MIGRATION_LIMIT,
    PG_DSN,
    PG_POOL_MAX,
    PG_POOL_MIN,
)

logger = logging.getLogger(__name__)
_metadata_app_root = Path(__file__).resolve().parents[1] / "app"
if str(_metadata_app_root) not in sys.path:
    sys.path.insert(0, str(_metadata_app_root))

try:
    from tqdm import tqdm
except Exception:  # pragma: no cover
    tqdm = None


def _load_checkpoint(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.warning("failed to parse checkpoint file: %s", path)
        return {}


def _save_checkpoint(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _clear_checkpoint(path: Path) -> None:
    if path.exists():
        path.unlink()


def _row_to_item(row: sqlite3.Row) -> dict[str, Any]:
    raw_tags = row["tags"] or "[]"
    try:
        tags = json.loads(raw_tags)
        if not isinstance(tags, list):
            tags = []
    except Exception:
        tags = []
    return {
        "id": int(row["id"]),
        "external_id": row["external_id"],
        "file_path": row["file_path"],
        "tags": tags,
        "source": row["source"],
        "cnt": int(row["cnt"] or 0),
        "avg": float(row["avg"] or 0.0),
        "created_at": row["created_at"],
    }


def migrate_sqlite_to_pg(
    sqlite_store,
    pg_store,
    *,
    batch_size: int = 1000,
    dry_run: bool = False,
    limit: int = 0,
    resume: bool = True,
    checkpoint_path: str | Path = MIGRATION_CHECKPOINT_PATH,
) -> dict[str, Any]:
    checkpoint_file = Path(checkpoint_path)
    start_ts = time.time()
    stats: dict[str, Any] = {
        "scanned": 0,
        "migrated": 0,
        "skipped": 0,
        "failed": 0,
        "resumed_from": 0,
        "elapsed_sec": 0.0,
        "dry_run": bool(dry_run),
        "limit": int(limit or 0),
    }

    sqlite_conn = sqlite_store._ensure_conn()  # noqa: SLF001
    sqlite_conn.row_factory = sqlite3.Row
    pg_store._ensure_connected()  # noqa: SLF001

    if resume:
        ckpt = _load_checkpoint(checkpoint_file)
        last_id = int(ckpt.get("last_id", 0) or 0)
    else:
        last_id = 0
    stats["resumed_from"] = last_id

    total_row = sqlite_conn.execute("SELECT COUNT(1) AS total FROM images").fetchone()
    total_available = int(total_row["total"] if total_row else 0)
    if total_available == 0:
        logger.info("sqlite metadata is empty, no migration needed")
        stats["elapsed_sec"] = round(time.time() - start_ts, 3)
        return stats

    planned_total = total_available if limit <= 0 else min(total_available, limit)
    progress = tqdm(total=planned_total, desc="sqlite->pg migration", unit="rows") if tqdm else None
    if progress and last_id > 0:
        progress.set_postfix_str(f"resume_from={last_id}")

    try:
        while True:
            if limit > 0 and stats["scanned"] >= limit:
                break
            effective_batch = batch_size
            if limit > 0:
                effective_batch = min(batch_size, limit - stats["scanned"])
                if effective_batch <= 0:
                    break

            rows = sqlite_conn.execute(
                """
                SELECT id, external_id, file_path, tags, source, cnt, avg, created_at
                FROM images
                WHERE id > ?
                ORDER BY id ASC
                LIMIT ?
                """,
                (last_id, effective_batch),
            ).fetchall()
            if not rows:
                break

            items = []
            failed_rows = 0
            for row in rows:
                try:
                    items.append(_row_to_item(row))
                except Exception as exc:
                    failed_rows += 1
                    logger.exception("row normalization failed for sqlite id=%s: %s", row["id"], exc)

            scanned_this_batch = len(rows)
            stats["scanned"] += scanned_this_batch
            stats["failed"] += failed_rows

            try:
                if not dry_run and items:
                    existing = pg_store.get_by_ids([item["id"] for item in items])
                    existing_ids = {int(item["id"]) for item in existing}
                    stats["skipped"] += len(existing_ids)
                    pg_store.batch_insert(items)
                    stats["migrated"] += max(len(items) - len(existing_ids), 0)
                elif dry_run:
                    stats["migrated"] += len(items)
            except Exception as exc:
                stats["failed"] += len(items)
                logger.exception("batch migration failed at sqlite id>%s: %s", last_id, exc)

            last_id = int(rows[-1]["id"])
            _save_checkpoint(
                checkpoint_file,
                {
                    "last_id": last_id,
                    "updated_at": int(time.time()),
                    "stats": stats,
                },
            )
            if progress:
                progress.update(scanned_this_batch)
    finally:
        if progress:
            progress.close()

    stats["elapsed_sec"] = round(time.time() - start_ts, 3)
    logger.info("sqlite->pg migration completed: %s", stats)
    return stats


def run_startup_migration_if_needed(*, current_backend: str) -> dict[str, Any] | None:
    if not MIGRATION_AUTO_RUN:
        logger.info("migration auto run disabled")
        return None
    if current_backend != "postgres":
        logger.info("current backend is %s, skip sqlite->pg migration", current_backend)
        return None
    from metadata.pg_store import PostgresMetadataStore  # lazy import
    from metadata.sqlite_store import SQLiteMetadataStore

    sqlite_store = SQLiteMetadataStore(METADATA_SQLITE_PATH)
    sqlite_store.init()
    pg_store = PostgresMetadataStore(PG_DSN, minconn=PG_POOL_MIN, maxconn=PG_POOL_MAX)
    pg_store.init()
    return migrate_sqlite_to_pg(
        sqlite_store,
        pg_store,
        batch_size=MIGRATION_BATCH_SIZE,
        dry_run=MIGRATION_DRY_RUN,
        limit=MIGRATION_LIMIT,
        resume=True,
        checkpoint_path=MIGRATION_CHECKPOINT_PATH,
    )


def reset_migration_checkpoint(path: str | Path = MIGRATION_CHECKPOINT_PATH) -> None:
    _clear_checkpoint(Path(path))
