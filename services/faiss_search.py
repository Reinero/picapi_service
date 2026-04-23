from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from config import CLIP_MODEL_ID, ENABLE_FAISS_SEARCH, FAISS_ID_MAP_PATH, FAISS_INDEX_PATH, GALLERY_DIR

logger = logging.getLogger(__name__)


class FaissSearchService:
    """FAISS-first search adapter.

    This service keeps FAISS retrieval isolated from metadata resolution.
    It returns hit ids (plus optional score), and caller resolves metadata via
    MetadataStore.get_by_ids(...) in one batch.
    """

    def __init__(
        self,
        *,
        enabled: bool = ENABLE_FAISS_SEARCH,
        index_path: str | Path = FAISS_INDEX_PATH,
        id_map_path: str | Path = FAISS_ID_MAP_PATH,
        clip_model_id: str = CLIP_MODEL_ID,
    ):
        self.enabled = enabled
        self.index_path = Path(index_path)
        self.id_map_path = Path(id_map_path)
        self.clip_model_id = clip_model_id
        self._faiss = None
        self._np = None
        self._index = None
        self._id_map: list[str] = []
        self._encoder = None
        self._ready = False
        self._load_error: str | None = None
        self._load()

    @property
    def ready(self) -> bool:
        return self._ready

    @property
    def load_error(self) -> str | None:
        return self._load_error

    def search_ids(self, query: str, top_k: int) -> list[int | str]:
        hits = self.search_with_scores(query=query, top_k=top_k)
        return [item["id"] for item in hits]

    def search_with_scores(self, query: str, top_k: int) -> list[dict[str, Any]]:
        if not self._ready or self._index is None:
            return []

        query_path = self._resolve_query_path(query)
        if query_path is None:
            return []

        try:
            vector = self._encode_query(query_path)
        except Exception as exc:  # pragma: no cover - runtime model issues
            logger.warning("faiss encode query failed: %s", exc)
            return []

        k = min(max(int(top_k), 1), int(self._index.ntotal))
        if k <= 0:
            return []

        scores, indices = self._index.search(vector, k)
        out: list[dict[str, Any]] = []
        for score, idx in zip(scores[0], indices[0]):
            if idx < 0:
                continue
            mapped = self._id_from_faiss_idx(int(idx))
            if mapped is None:
                continue
            out.append({"id": mapped, "score": float(score)})
        return out

    def _load(self) -> None:
        if not self.enabled:
            self._load_error = "faiss search disabled by config"
            return
        if not self.index_path.is_file() or not self.id_map_path.is_file():
            self._load_error = "faiss index or id_map file not found"
            return
        try:
            import faiss  # type: ignore
            import numpy as np  # type: ignore
        except Exception as exc:
            self._load_error = f"faiss/numpy import failed: {exc}"
            return

        self._faiss = faiss
        self._np = np
        try:
            self._index = faiss.read_index(str(self.index_path))
            with self.id_map_path.open(encoding="utf-8") as f:
                raw = json.load(f)
            self._id_map = list(raw)
            self._ready = self._index is not None and int(self._index.ntotal) > 0 and len(self._id_map) > 0
            if not self._ready:
                self._load_error = "faiss index loaded but empty"
        except Exception as exc:
            self._load_error = f"faiss load failed: {exc}"
            self._ready = False

    def _resolve_query_path(self, query: str) -> Path | None:
        q = (query or "").strip()
        if not q:
            return None
        p = Path(q)
        if p.is_file():
            return p
        candidate = (GALLERY_DIR / q).resolve()
        if candidate.is_file():
            return candidate
        return None

    def _encode_query(self, image_path: Path):
        if self._encoder is None:
            self._encoder = _ImageEncoder(model_name=self.clip_model_id)
        vec = self._encoder.encode_path(image_path)
        return vec

    def _id_from_faiss_idx(self, idx: int) -> int | str | None:
        if idx < 0 or idx >= len(self._id_map):
            return None
        raw = self._id_map[idx]
        if isinstance(raw, int):
            return raw
        if isinstance(raw, str):
            try:
                return int(raw)
            except ValueError:
                return raw
        return str(raw)


def example_faiss_metadata_flow() -> list[dict[str, Any]]:
    """Example integration flow.

    1) FAISS retrieves ids + scores
    2) MetadataStore resolves ids in one batch
    """
    from services.search import resolve_search_hits_with_scores

    service = FaissSearchService()
    hits = service.search_with_scores(query="cats/a.jpg", top_k=10)
    return resolve_search_hits_with_scores(hits)


class _ImageEncoder:
    """Lazy CLIP image encoder (compatible with image_search setup)."""

    def __init__(self, model_name: str):
        import torch
        from modelscope import snapshot_download
        from transformers import ChineseCLIPModel, ChineseCLIPProcessor

        self._torch = torch
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        model_dir = snapshot_download(model_name)
        self.model = ChineseCLIPModel.from_pretrained(model_dir).to(self.device)
        self.processor = ChineseCLIPProcessor.from_pretrained(model_dir)
        self.model.eval()

    def encode_path(self, image_path: Path):
        from PIL import Image

        image = Image.open(image_path).convert("RGB")
        with self._torch.inference_mode():
            inputs = self.processor(images=image, return_tensors="pt")
            inputs = {k: v.to(self.device) for k, v in inputs.items()}
            feats = self.model.get_image_features(pixel_values=inputs["pixel_values"])
            feats = feats / feats.norm(dim=-1, keepdim=True)
            return feats.cpu().numpy().astype("float32")
