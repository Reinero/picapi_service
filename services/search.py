from __future__ import annotations

import logging
from typing import Any

from services.metadata_store import get_store

logger = logging.getLogger(__name__)


def resolve_search_hits(ids: list[str | int]) -> list[dict[str, Any]]:
    """Resolve FAISS hit ids into metadata rows in one batch query.

    This function does not run FAISS itself. It only converts ids returned by
    upstream retrieval into final metadata payloads.
    """
    if not ids:
        return []

    store = get_store()
    rows = store.get_by_ids(ids)
    by_id = {int(row["id"]): row for row in rows if "id" in row}
    by_external_id = {str(row["external_id"]): row for row in rows if row.get("external_id")}

    result: list[dict[str, Any]] = []
    missing_count = 0
    for raw_id in ids:
        if isinstance(raw_id, int):
            row = by_id.get(raw_id)
        else:
            row = by_external_id.get(str(raw_id))
        if not row:
            missing_count += 1
            continue
        result.append(
            {
                "id": int(row["id"]),
                "external_id": row.get("external_id"),
                "file_path": row.get("file_path"),
                "tags": row.get("tags", []),
                "source": row.get("source"),
                "cnt": int(row.get("cnt", 0)),
                "avg": float(row.get("avg", 0.0)),
                "created_at": row.get("created_at"),
            }
        )

    if missing_count:
        logger.info("resolve_search_hits skipped %s missing ids", missing_count)
    return result


def resolve_search_hits_with_scores(hits: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Resolve FAISS hits that already carry score.

    Input shape:
        [{"id": <int|str>, "score": <float>}, ...]
    """
    if not hits:
        return []
    ids = [item["id"] for item in hits if "id" in item]
    rows = resolve_search_hits(ids)
    row_by_id = {int(row["id"]): row for row in rows}
    row_by_external = {str(row["external_id"]): row for row in rows if row.get("external_id")}

    result: list[dict[str, Any]] = []
    for item in hits:
        raw_id = item.get("id")
        score = float(item.get("score", 0.0))
        if isinstance(raw_id, int):
            row = row_by_id.get(raw_id)
        else:
            row = row_by_external.get(str(raw_id))
        if not row:
            continue
        merged = dict(row)
        merged["score"] = score
        result.append(merged)
    return result


def example_faiss_flow() -> list[dict[str, Any]]:
    """Example:
    faiss_ids = [1234567890, "gallery/cats/a.jpg", "gallery/dogs/b.jpg"]
    return resolve_search_hits(faiss_ids)
    """
    faiss_ids: list[str | int] = [1234567890, "gallery/cats/a.jpg", "gallery/dogs/b.jpg"]
    return resolve_search_hits(faiss_ids)
