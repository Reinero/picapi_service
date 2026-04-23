from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime

from psycopg2 import pool
from psycopg2.extras import RealDictCursor, execute_values

from .base import MetadataItem, MetadataStore, StoreListResult, StoreResult
from .utils import to_int64_id

POSTGRES_INIT_SQL = """
CREATE TABLE IF NOT EXISTS images (
    id BIGINT PRIMARY KEY,
    external_id TEXT UNIQUE,
    file_path TEXT,
    tags TEXT[],
    source VARCHAR(50),
    cnt INT DEFAULT 0,
    avg FLOAT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_images_tags ON images USING GIN(tags);
CREATE INDEX IF NOT EXISTS idx_images_external_id ON images(external_id);
""".strip()


class PostgresMetadataStore(MetadataStore):
    """PostgreSQL implementation of MetadataStore backed by connection pool."""

    def __init__(self, dsn: str, minconn: int = 1, maxconn: int = 5):
        self.dsn = dsn
        self.minconn = minconn
        self.maxconn = maxconn
        self._pool: pool.SimpleConnectionPool | None = None

    def init(self) -> StoreResult:
        if self._pool is None:
            self._pool = pool.SimpleConnectionPool(self.minconn, self.maxconn, self.dsn)
        with self._cursor(commit=True) as cur:
            cur.execute(POSTGRES_INIT_SQL)
        return {"ok": True, "backend": "postgres"}

    def insert(self, item: MetadataItem) -> StoreResult:
        required = ("external_id", "file_path", "tags", "source")
        missing = [key for key in required if key not in item]
        if missing:
            raise ValueError(f"missing required fields: {', '.join(missing)}")
        external_id = str(item["external_id"])
        internal_id = int(item.get("id", to_int64_id(external_id)))
        created_at = self._normalize_created_at(item.get("created_at"))
        with self._cursor(commit=True) as cur:
            cur.execute(
                """
                INSERT INTO images(id, external_id, file_path, tags, source, cnt, avg, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, COALESCE(%s, CURRENT_TIMESTAMP))
                ON CONFLICT (id) DO UPDATE SET
                    external_id=EXCLUDED.external_id,
                    file_path=EXCLUDED.file_path,
                    tags=EXCLUDED.tags,
                    source=EXCLUDED.source,
                    cnt=EXCLUDED.cnt,
                    avg=EXCLUDED.avg,
                    created_at=EXCLUDED.created_at
                """,
                (
                    internal_id,
                    external_id,
                    item["file_path"],
                    item.get("tags", []),
                    item["source"],
                    int(item.get("cnt", 0)),
                    float(item.get("avg", 0.0)),
                    created_at,
                ),
            )
        return {"ok": True, "affected": 1}

    def batch_insert(self, items: list[MetadataItem]) -> StoreResult:
        if not items:
            return {"ok": True, "affected": 0}
        rows = []
        for item in items:
            required = ("external_id", "file_path", "tags", "source")
            missing = [key for key in required if key not in item]
            if missing:
                raise ValueError(f"missing required fields: {', '.join(missing)}")
            external_id = str(item["external_id"])
            rows.append(
                (
                    int(item.get("id", to_int64_id(external_id))),
                    external_id,
                    item["file_path"],
                    item.get("tags", []),
                    item["source"],
                    int(item.get("cnt", 0)),
                    float(item.get("avg", 0.0)),
                    self._normalize_created_at(item.get("created_at")),
                )
            )
        with self._cursor(commit=True) as cur:
            execute_values(
                cur,
                """
                INSERT INTO images(id, external_id, file_path, tags, source, cnt, avg, created_at)
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                    external_id=EXCLUDED.external_id,
                    file_path=EXCLUDED.file_path,
                    tags=EXCLUDED.tags,
                    source=EXCLUDED.source,
                    cnt=EXCLUDED.cnt,
                    avg=EXCLUDED.avg,
                    created_at=EXCLUDED.created_at
                """,
                rows,
                template="(%s,%s,%s,%s,%s,%s,%s,COALESCE(%s, CURRENT_TIMESTAMP))",
            )
        return {"ok": True, "affected": len(rows)}

    def get_by_id(self, id_value: str | int) -> StoreResult:
        internal_id = self._normalize_id(id_value)
        with self._cursor() as cur:
            cur.execute(
                "SELECT id, external_id, file_path, tags, source, cnt, avg, created_at FROM images WHERE id=%s",
                (internal_id,),
            )
            row = cur.fetchone()
        if not row:
            return {}
        return self._row_to_item(row)

    def get_by_ids(self, ids: list[str | int]) -> StoreListResult:
        if not ids:
            return []
        normalized_ids = [self._normalize_id(id_value) for id_value in ids]
        with self._cursor() as cur:
            cur.execute(
                "SELECT id, external_id, file_path, tags, source, cnt, avg, created_at FROM images WHERE id = ANY(%s)",
                (normalized_ids,),
            )
            rows = cur.fetchall()
        return [self._row_to_item(row) for row in rows]

    def filter_by_tags(self, tags: list[str]) -> StoreListResult:
        if not tags:
            return []
        with self._cursor() as cur:
            cur.execute(
                """
                SELECT id, external_id, file_path, tags, source, cnt, avg, created_at
                FROM images
                WHERE tags && %s::text[]
                ORDER BY created_at DESC
                """,
                (tags,),
            )
            rows = cur.fetchall()
        return [self._row_to_item(row) for row in rows]

    def count(self) -> StoreResult:
        with self._cursor() as cur:
            cur.execute("SELECT COUNT(1) AS total FROM images")
            row = cur.fetchone()
        return {"count": int((row or {}).get("total", 0))}

    def close(self) -> StoreResult:
        if self._pool is not None:
            self._pool.closeall()
            self._pool = None
        return {"ok": True, "backend": "postgres"}

    @contextmanager
    def _cursor(self, commit: bool = False):
        self._ensure_connected()
        assert self._pool is not None
        conn = self._pool.getconn()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                yield cur
            if commit:
                conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._pool.putconn(conn)

    def _ensure_connected(self) -> None:
        if self._pool is None:
            raise RuntimeError("PostgreSQL store is not initialized. Call init() first.")

    def _normalize_id(self, id_value: str | int) -> int:
        if isinstance(id_value, int):
            return id_value
        return to_int64_id(id_value)

    def _row_to_item(self, row: dict) -> dict:
        created_at = row.get("created_at")
        if isinstance(created_at, datetime):
            created_at = created_at.isoformat()
        return {
            "id": int(row["id"]),
            "external_id": row["external_id"],
            "file_path": row["file_path"],
            "tags": row.get("tags") or [],
            "source": row.get("source"),
            "cnt": int(row.get("cnt", 0)),
            "avg": float(row.get("avg", 0.0)),
            "created_at": created_at,
        }

    def _normalize_created_at(self, value):
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value)
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except ValueError:
                return None
        return None
