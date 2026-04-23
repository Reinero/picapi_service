from __future__ import annotations

import hashlib


def to_int64_id(value: str) -> int:
    """Convert external string id to a stable signed BIGINT-compatible value.

    We use the first 16 hex chars (64-bit) of SHA-256 digest. Collisions are
    extremely unlikely but still theoretically possible for truncated hashes.
    """
    digest = hashlib.sha256(value.encode("utf-8", errors="replace")).hexdigest()[:16]
    as_uint64 = int(digest, 16)
    # Keep values in signed int64 range to stay compatible with BIGINT.
    return as_uint64 & 0x7FFF_FFFF_FFFF_FFFF
