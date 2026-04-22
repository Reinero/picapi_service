import os
from pathlib import Path

PICK_BIAS = os.environ.get("PICK_BIAS", "min").lower()
PICK_BIAS_ALPHA = float(os.environ.get("PICK_BIAS_ALPHA", "1.0"))
GALLERY_DIR = Path(os.environ.get("GALLERY_DIR", "/data/gallery")).resolve()
STATIC_PREFIX = os.environ.get("STATIC_PREFIX", "/static")
ALLOWED_SUFFIXES = tuple(
    s.strip().lower() for s in os.environ.get("ALLOWED_SUFFIXES", ".jpg,.jpeg,.png,.gif,.webp").split(",")
)
RECURSIVE = os.environ.get("RECURSIVE", "true").lower() in {"1", "true", "yes"}
DB_PATH = Path("/data/db/picapi.sqlite")
DB_BACKEND = os.environ.get("DB_BACKEND", "sqlite").strip().lower()
USE_PG = os.environ.get("USE_PG", "true").strip().lower() in {"1", "true", "yes", "on"}
ALLOW_SQLITE_FALLBACK = os.environ.get("ALLOW_SQLITE_FALLBACK", "false").strip().lower() in {"1", "true", "yes", "on"}
METADATA_SQLITE_PATH = Path(os.environ.get("METADATA_SQLITE_PATH", "/data/db/metadata.sqlite"))
PG_DSN = os.environ.get("PG_DSN", "")
PG_CONNECT_TIMEOUT_SEC = int(os.environ.get("PG_CONNECT_TIMEOUT_SEC", "3"))
PG_POOL_MIN = int(os.environ.get("PG_POOL_MIN", "1"))
PG_POOL_MAX = int(os.environ.get("PG_POOL_MAX", "5"))
MIGRATION_BATCH_SIZE = int(os.environ.get("MIGRATION_BATCH_SIZE", "1000"))
MIGRATION_CHECKPOINT_PATH = Path(
    os.environ.get("MIGRATION_CHECKPOINT_PATH", "/data/db/metadata_migration_checkpoint.json")
)
MIGRATION_AUTO_RUN = os.environ.get("MIGRATION_AUTO_RUN", "true").strip().lower() in {"1", "true", "yes", "on"}
MIGRATION_DRY_RUN = os.environ.get("MIGRATION_DRY_RUN", "false").strip().lower() in {"1", "true", "yes", "on"}
MIGRATION_LIMIT = int(os.environ.get("MIGRATION_LIMIT", "0"))
ENABLE_FAISS_SEARCH = os.environ.get("ENABLE_FAISS_SEARCH", "false").strip().lower() in {"1", "true", "yes", "on"}
FAISS_INDEX_PATH = Path(os.environ.get("FAISS_INDEX_PATH", "/data/faiss/images.faiss"))
FAISS_ID_MAP_PATH = Path(os.environ.get("FAISS_ID_MAP_PATH", "/data/faiss/id_map.json"))
CLIP_MODEL_ID = os.environ.get("CLIP_MODEL_ID", "AI-ModelScope/chinese-clip-vit-large-patch14")
WRITE_META_MIN_COUNT = int(os.environ.get("WRITE_META_MIN_COUNT", "1"))
