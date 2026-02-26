# ABOUTME: Tests for Milvus Lite vector store wrapper.
# ABOUTME: Verifies upsert, search, and incremental update operations.

import pytest
from living_graph.vector_store import VectorStore


@pytest.fixture
def store(tmp_path):
    """Create a temporary vector store."""
    db_path = str(tmp_path / "test_vectors.db")
    return VectorStore(db_path=db_path, dimension=768)


def test_upsert_and_search(store):
    """Should store vectors and retrieve nearest neighbors."""
    store.upsert("page_1", [0.1] * 768, {"title": "Test Page 1"})
    store.upsert("page_2", [0.9] * 768, {"title": "Test Page 2"})

    results = store.search([0.1] * 768, top_k=2)
    assert len(results) >= 1
    assert results[0]["id"] == "page_1"


def test_upsert_updates_existing(store):
    """Upserting same ID should update, not duplicate."""
    store.upsert("page_1", [0.1] * 768, {"title": "Version 1"})
    store.upsert("page_1", [0.2] * 768, {"title": "Version 2"})

    results = store.search([0.2] * 768, top_k=5)
    ids = [r["id"] for r in results]
    assert ids.count("page_1") == 1


def test_get_all_vectors(store):
    """Should retrieve all stored vectors for clustering."""
    store.upsert("page_1", [0.1] * 768, {"title": "Page 1"})
    store.upsert("page_2", [0.9] * 768, {"title": "Page 2"})

    all_vecs = store.get_all()
    assert len(all_vecs) == 2
    assert all_vecs[0]["id"] in ("page_1", "page_2")
    assert len(all_vecs[0]["vector"]) == 768


def test_delete(store):
    """Should remove a vector by ID."""
    store.upsert("page_1", [0.1] * 768, {"title": "Page 1"})
    store.delete("page_1")

    results = store.search([0.1] * 768, top_k=5)
    ids = [r["id"] for r in results]
    assert "page_1" not in ids


def test_count(store):
    """Should report correct count of stored vectors."""
    assert store.count() == 0
    store.upsert("page_1", [0.1] * 768, {"title": "P1"})
    assert store.count() == 1
    store.upsert("page_2", [0.2] * 768, {"title": "P2"})
    assert store.count() == 2
