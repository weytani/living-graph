# ABOUTME: Milvus Lite wrapper for surveyor vector storage.
# ABOUTME: File-based vector DB — no server process needed.

from __future__ import annotations

from pymilvus import MilvusClient


COLLECTION_NAME = "page_embeddings"


class VectorStore:
    """Wrapper around Milvus Lite for page embedding storage."""

    def __init__(self, db_path: str, dimension: int = 768):
        self._client = MilvusClient(db_path)
        self._dimension = dimension
        self._collection = COLLECTION_NAME

        if not self._client.has_collection(self._collection):
            self._client.create_collection(
                collection_name=self._collection,
                dimension=dimension,
                id_type="str",
                max_length=256,
            )

    def upsert(self, page_id: str, vector: list[float], metadata: dict) -> None:
        """Insert or update a page embedding."""
        self._client.upsert(
            collection_name=self._collection,
            data=[{"id": page_id, "vector": vector, **metadata}],
        )

    def search(self, query_vector: list[float], top_k: int = 10) -> list[dict]:
        """Find nearest neighbors to a query vector."""
        results = self._client.search(
            collection_name=self._collection,
            data=[query_vector],
            limit=top_k,
            output_fields=["title"],
        )
        return [
            {
                "id": hit["id"],
                "distance": hit["distance"],
                "title": hit.get("entity", {}).get("title", ""),
            }
            for hit in results[0]
        ]

    def get_all(self) -> list[dict]:
        """Retrieve all vectors for clustering."""
        results = self._client.query(
            collection_name=self._collection,
            filter='id != ""',
            output_fields=["vector", "title"],
        )
        return [
            {"id": r["id"], "vector": r["vector"], "title": r.get("title", "")}
            for r in results
        ]

    def delete(self, page_id: str) -> None:
        """Remove a page embedding by ID."""
        self._client.delete(
            collection_name=self._collection,
            ids=[page_id],
        )

    def count(self) -> int:
        """Return the number of stored embeddings."""
        stats = self._client.get_collection_stats(self._collection)
        return stats["row_count"]
