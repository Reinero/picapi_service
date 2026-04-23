from __future__ import annotations

from services.metadata_store import current_backend, get_store


def run_demo() -> None:
    store = get_store()
    print(f"active metadata backend: {current_backend()}")

    store.insert(
        {
            "external_id": "demo/cat-1.jpg",
            "file_path": "demo/cat-1.jpg",
            "tags": ["cat", "cute"],
            "source": "demo",
        }
    )
    store.batch_insert(
        [
            {
                "external_id": "demo/cat-2.jpg",
                "file_path": "demo/cat-2.jpg",
                "tags": ["cat"],
                "source": "demo",
            },
            {
                "external_id": "demo/dog-1.jpg",
                "file_path": "demo/dog-1.jpg",
                "tags": ["dog"],
                "source": "demo",
            },
        ]
    )

    rows = store.get_by_ids(["demo/cat-1.jpg", "demo/dog-1.jpg"])
    print("get_by_ids:", rows)
    print("filter_by_tags(cat):", store.filter_by_tags(["cat"]))
    print("count:", store.count())


if __name__ == "__main__":
    run_demo()
