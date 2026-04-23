from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TypedDict


class MetadataItem(TypedDict, total=False):
    id: int
    external_id: str
    file_path: str
    tags: list[str]
    source: str
    cnt: int
    avg: float
    created_at: int | str | None


StoreResult = dict
StoreListResult = list[dict]


class MetadataStore(ABC):
    """Database-agnostic metadata storage contract."""

    @abstractmethod
    def init(self) -> StoreResult:
        """Initialize storage (tables/indexes) and return operation status."""

    @abstractmethod
    def insert(self, item: MetadataItem) -> StoreResult:
        """Insert or upsert one metadata item."""

    @abstractmethod
    def batch_insert(self, items: list[MetadataItem]) -> StoreResult:
        """Insert or upsert multiple metadata items in one operation."""

    @abstractmethod
    def get_by_id(self, id_value: str | int) -> StoreResult:
        """Get one item by id; return empty dict when missing."""

    @abstractmethod
    def get_by_ids(self, ids: list[str | int]) -> StoreListResult:
        """Get many items by ids."""

    @abstractmethod
    def filter_by_tags(self, tags: list[str]) -> StoreListResult:
        """Return items that match all specified tags."""

    @abstractmethod
    def count(self) -> StoreResult:
        """Return total item count as {'count': int}."""

    @abstractmethod
    def close(self) -> StoreResult:
        """Release resources held by the store."""
