from __future__ import annotations

import logging
import sys
from pathlib import Path
from threading import Lock
from typing import TYPE_CHECKING

from config import ALLOW_SQLITE_FALLBACK, METADATA_SQLITE_PATH, PG_CONNECT_TIMEOUT_SEC, PG_DSN, PG_POOL_MAX, PG_POOL_MIN, USE_PG

_metadata_app_root = Path(__file__).resolve().parents[1] / "app"
if str(_metadata_app_root) not in sys.path:
    sys.path.insert(0, str(_metadata_app_root))

from metadata.base import MetadataStore  # noqa: E402
from metadata.sqlite_store import SQLiteMetadataStore  # noqa: E402

if TYPE_CHECKING:
    from metadata.pg_store import PostgresMetadataStore

logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self):
        self._lock = Lock()
        self._store: MetadataStore | None = None
        self._backend = "unknown"

    def get_store(self) -> MetadataStore:
        with self._lock:
            if self._store is None:
                self._store, self._backend = self._select_store()
                logger.info("DatabaseManager selected backend: %s", self._backend)
            return self._store

    def current_backend(self) -> str:
        with self._lock:
            return self._backend

    def _select_store(self) -> tuple[MetadataStore, str]:
        if not USE_PG:
            logger.info("USE_PG=false, using SQLiteMetadataStore.")
            store = SQLiteMetadataStore(METADATA_SQLITE_PATH)
            store.init()
            return store, "sqlite"

        if not PG_DSN:
            message = "USE_PG=true but PG_DSN is empty."
            return self._fallback_or_raise(message)

        pg_dsn = _with_connect_timeout(PG_DSN, PG_CONNECT_TIMEOUT_SEC)
        try:
            from metadata.pg_store import PostgresMetadataStore  # noqa: E402

            pg_store = PostgresMetadataStore(pg_dsn, minconn=PG_POOL_MIN, maxconn=PG_POOL_MAX)
            pg_store.init()
            logger.info("PostgreSQL is available, using PostgresMetadataStore.")
            return pg_store, "postgres"
        except Exception as exc:
            return self._fallback_or_raise(f"PostgreSQL unavailable ({exc})", exc)

    def _fallback_or_raise(self, reason: str, exc: Exception | None = None) -> tuple[MetadataStore, str]:
        if not ALLOW_SQLITE_FALLBACK:
            logger.error(
                "%s SQLite fallback disabled (ALLOW_SQLITE_FALLBACK=false), failing fast.",
                reason,
            )
            if exc is not None:
                raise RuntimeError(reason) from exc
            raise RuntimeError(reason)
        logger.warning("%s fallback to SQLiteMetadataStore.", reason)
        if exc is not None:
            logger.exception("PostgreSQL initialization failure detail")
        store = SQLiteMetadataStore(METADATA_SQLITE_PATH)
        store.init()
        return store, "sqlite"


def _with_connect_timeout(dsn: str, timeout_sec: int) -> str:
    if "connect_timeout" in dsn:
        return dsn
    timeout = max(int(timeout_sec), 1)
    if "://" in dsn:
        connector = "&" if "?" in dsn else "?"
        return f"{dsn}{connector}connect_timeout={timeout}"
    return f"{dsn} connect_timeout={timeout}"


_manager = DatabaseManager()


def get_store() -> MetadataStore:
    return _manager.get_store()


def current_backend() -> str:
    return _manager.current_backend()


def get_metadata_store() -> MetadataStore:
    """Backward-compatible alias for existing callers."""
    return get_store()
