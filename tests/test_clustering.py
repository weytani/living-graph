# ABOUTME: Tests for the clustering module — HDBSCAN + Leiden.
# ABOUTME: Verifies semantic and structural cluster detection and merging.

import numpy as np
from living_graph.clustering import (
    semantic_clusters,
    structural_clusters,
    merge_clusters,
)


def test_semantic_clusters_groups_similar_vectors():
    """HDBSCAN should group nearby vectors into clusters."""
    # Two clusters pointing in different directions:
    # Group A: first 5 dims high, last 5 dims low
    # Group B: first 5 dims low, last 5 dims high
    # This ensures separation survives L2-normalization.
    rng = np.random.RandomState(42)
    vectors = {}
    for i in range(5):
        v = rng.normal(0, 0.3, 10)
        v[:5] += 5.0  # push first half high
        vectors[f"page_a{i}"] = v.tolist()
    for i in range(5):
        v = rng.normal(0, 0.3, 10)
        v[5:] += 5.0  # push second half high
        vectors[f"page_b{i}"] = v.tolist()

    clusters = semantic_clusters(vectors, min_cluster_size=3, min_samples=2)

    # Should find at least 2 clusters
    assert len(clusters) >= 2

    # Pages in same cluster should be from same group
    for cluster in clusters:
        prefixes = {pid.split("_")[0] for pid in cluster}
        assert len(prefixes) == 1, f"Mixed cluster: {cluster}"


def test_semantic_clusters_handles_noise():
    """Isolated points should not form clusters."""
    rng = np.random.RandomState(42)
    vectors = {}
    for i in range(5):
        v = rng.normal(0, 0.3, 10)
        v[:5] += 5.0  # tight cluster in first-half direction
        vectors[f"page_a{i}"] = v.tolist()
    # One lonely outlier in a completely different direction
    vectors["page_lonely"] = [1.0, -1.0, 1.0, -1.0, 1.0, -1.0, 1.0, -1.0, 1.0, -1.0]

    clusters = semantic_clusters(vectors, min_cluster_size=3, min_samples=2)

    all_pages = {pid for c in clusters for pid in c}
    assert "page_lonely" not in all_pages


def test_semantic_clusters_too_few_points():
    """Should return empty list when fewer points than min_cluster_size."""
    vectors = {
        "page_a": [0.1] * 10,
        "page_b": [0.9] * 10,
    }
    clusters = semantic_clusters(vectors, min_cluster_size=3)
    assert clusters == []


def test_structural_clusters_from_refs():
    """Leiden should find communities in the ref graph."""
    # Two disconnected cliques: a1-a2-a3 (triangle), b1-b2-b3 (triangle)
    refs = {
        "page_a1": {"page_a2", "page_a3"},
        "page_a2": {"page_a1", "page_a3"},
        "page_a3": {"page_a1", "page_a2"},
        "page_b1": {"page_b2", "page_b3"},
        "page_b2": {"page_b1", "page_b3"},
        "page_b3": {"page_b1", "page_b2"},
    }
    clusters = structural_clusters(refs)
    assert len(clusters) >= 2

    # Each cluster should contain only one group
    for cluster in clusters:
        prefixes = {pid.split("_")[0] for pid in cluster}
        assert len(prefixes) == 1, f"Mixed cluster: {cluster}"


def test_structural_clusters_empty_refs():
    """Should return empty list for empty ref graph."""
    clusters = structural_clusters({})
    assert clusters == []


def test_structural_clusters_no_edges():
    """Pages with no connections should not form meaningful clusters."""
    refs = {
        "page_a": set(),
        "page_b": set(),
    }
    # No edges means no communities with >= 2 members
    clusters = structural_clusters(refs)
    # Either empty or each page is isolated (filtered out as size < 2)
    for cluster in clusters:
        assert len(cluster) >= 2


def test_merge_clusters_intersection():
    """Pages in both semantic AND structural clusters are high-confidence."""
    sem = [{"page_a", "page_b", "page_c"}, {"page_d", "page_e"}]
    struct = [{"page_a", "page_b"}, {"page_d", "page_f"}]

    merged = merge_clusters(sem, struct)

    # page_a and page_b overlap in both -> high confidence
    assert any("page_a" in c and "page_b" in c for c in merged)


def test_merge_clusters_no_overlap():
    """Disjoint clusters should still be returned (lower confidence)."""
    sem = [{"page_a", "page_b"}]
    struct = [{"page_c", "page_d"}]

    merged = merge_clusters(sem, struct)
    assert len(merged) >= 2  # Both preserved


def test_merge_clusters_empty_inputs():
    """Should handle empty cluster lists gracefully."""
    assert merge_clusters([], []) == []
    assert merge_clusters([{"page_a", "page_b"}], []) == [{"page_a", "page_b"}]
    assert merge_clusters([], [{"page_c", "page_d"}]) == [{"page_c", "page_d"}]
