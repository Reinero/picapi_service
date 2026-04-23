from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from .base import MetadataItem, MetadataStore, StoreListResult, StoreResult
from .utils import to_int64_id

SQLITE_INIT_SQL = """
CREATE TABLE IF NOT EXISTS images (
    id INTEGER PRIMARY KEY,
    external_id TEXT UNIQUE,
    file_path TEXT,
    tags TEXT,
    source TEXT,
    cnt INTEGER DEFAULT 0,
    avg REAL DEFAULT 0,
    created_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_images_external_id ON images(external_id);
CREATE INDEX IF NOT EXISTS idx_images_source ON images(source);
""".strip()


class SQLiteMetadataStore(MetadataStore):
    """SQLite implementation of MetadataStore.

    Example:
        store = SQLiteMetadataStore(Path("data/app.db"))
        store.init()
        store.insert(
            {
                "external_id": "cats/a.jpg",
                "file_path": "cats/a.jpg",
                "tags": ["cat", "cute"],
                "source": "gallery",
            }
        )
        store.batch_insert(
            [
                {
                    "external_id": "cats/b.jpg",
                    "file_path": "cats/b.jpg",
                    "tags": ["cat"],
                    "source": "gallery",
                }
            ]
        )
        print(store.get_by_id("cats/a.jpg"))
        print(store.get_by_ids(["cats/a.jpg", "cats/b.jpg"]))
        print(store.filter_by_tags(["cat"]))
        print(store.count())
        store.close()
    """

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self._conn: sqlite3.Connection | None = None

    def init(self) -> StoreResult:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL;")
            self._conn.execute("PRAGMA synchronous=NORMAL;")
            self._conn.execute("PRAGMA busy_timeout=5000;")

        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS images (
                id INTEGER PRIMARY KEY,
                external_id TEXT UNIQUE,
                file_path TEXT,
                tags TEXT,
                source TEXT,
                cnt INTEGER NOT NULL DEFAULT 0,
                avg REAL NOT NULL DEFAULT 0.0,
                created_at INTEGER
            )
            """
        )
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_images_external_id ON images(external_id)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_images_source ON images(source)")
        self._conn.commit()
        return {"ok": True}

    def insert(self, item: MetadataItem) -> StoreResult:
        conn = self._ensure_conn()
        self._upsert_item(conn, item)
        conn.commit()
        return {"ok": True, "affected": 1}

    def batch_insert(self, items: list[MetadataItem]) -> StoreResult:
        conn = self._ensure_conn()
        for item in items:
            self._upsert_item(conn, item)
        conn.commit()
        return {"ok": True, "affected": len(items)}

    def get_by_id(self, id_value: str | int) -> StoreResult:
        conn = self._ensure_conn()
        internal_id = self._normalize_id(id_value)
        row = conn.execute(
            "SELECT id, external_id, file_path, tags, source, cnt, avg, created_at FROM images WHERE id=?",
            (internal_id,),
        ).fetchone()
        if not row:
            return {}
        return self._row_to_item(row)

    def get_by_ids(self, ids: list[str | int]) -> StoreListResult:
        if not ids:
            return []
        normalized_ids = [self._normalize_id(raw_id) for raw_id in ids]
        conn = self._ensure_conn()
        placeholders = ",".join("?" * len(normalized_ids))
        rows = conn.execute(
            f"SELECT id, external_id, file_path, tags, source, cnt, avg, created_at FROM images WHERE id IN ({placeholders})",
            tuple(normalized_ids),
        ).fetchall()
        return [self._row_to_item(row) for row in rows]

    def filter_by_tags(self, tags: list[str]) -> StoreListResult:
        if not tags:
            return []
        conn = self._ensure_conn()
        first_tag = tags[0]
        rows = conn.execute(
            """
            SELECT id, external_id, file_path, tags, source, cnt, avg, created_at
            FROM images
            WHERE tags LIKE ?
            ORDER BY created_at DESC
            """,
            (f'%"{first_tag}"%',),
        ).fetchall()
        tags_set = {tag.lower() for tag in tags}
        result: StoreListResult = []
        for row in rows:
            item = self._row_to_item(row)
            row_tags = {tag.lower() for tag in item.get("tags", [])}
            if tags_set.issubset(row_tags):
                result.append(item)
        return result

    def count(self) -> StoreResult:
        conn = self._ensure_conn()
        row = conn.execute("SELECT COUNT(1) AS total FROM images").fetchone()
        return {"count": int(row["total"] if row else 0)}

    def close(self) -> StoreResult:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
        return {"ok": True}

    def _ensure_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self.init()
        if self._conn is None:
            raise RuntimeError("sqlite connection not initialized")
        return self._conn

    def _upsert_item(self, conn: sqlite3.Connection, item: MetadataItem) -> None:
        required = ("external_id", "file_path", "tags", "source")
        missing = [key for key in required if key not in item]
        if missing:
            raise ValueError(f"missing required fields: {', '.join(missing)}")
        external_id = str(item["external_id"])
        internal_id = int(item.get("id", to_int64_id(external_id)))
        tags_json = json.dumps(item.get("tags", []), ensure_ascii=True)

        conn.execute(
            """
            INSERT INTO images(id, external_id, file_path, tags, source, cnt, avg, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                external_id=excluded.external_id,
                file_path=excluded.file_path,
                tags=excluded.tags,
                source=excluded.source,
                cnt=excluded.cnt,
                avg=excluded.avg,
                created_at=excluded.created_at
            """,
            (
                internal_id,
                external_id,
                item["file_path"],
                tags_json,
                item["source"],
                int(item.get("cnt", 0)),
                float(item.get("avg", 0.0)),
                int(item.get("created_at") or 0),
            ),
        )

    def _row_to_item(self, row: sqlite3.Row) -> dict:
        raw_tags = row["tags"] if "tags" in row.keys() else "[]"
        try:
            tags = json.loads(raw_tags) if raw_tags else []
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

    def _normalize_id(self, id_value: str | int) -> int:
        if isinstance(id_value, int):
            return id_value
        return to_int64_id(id_value)
