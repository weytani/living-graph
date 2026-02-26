# ABOUTME: Semantic (HDBSCAN) and structural (Leiden) clustering for the surveyor.
# ABOUTME: Finds groups of related pages by vector similarity and link structure.

from __future__ import annotations

import numpy as np
import hdbscan
import igraph as ig
import leidenalg


def semantic_clusters(
    vectors: dict[str, list[float]],
    min_cluster_size: int = 3,
    min_samples: int = 2,
) -> list[set[str]]:
    """Cluster page vectors using HDBSCAN.

    Args:
        vectors: {page_id: embedding_vector} dict.
        min_cluster_size: Minimum pages to form a cluster.
        min_samples: HDBSCAN min_samples parameter.

    Returns:
        List of sets, each set containing page IDs in a cluster.
        Noise points (label -1) are excluded.
    """
    if len(vectors) < min_cluster_size:
        return []

    ids = list(vectors.keys())
    matrix = np.array([vectors[pid] for pid in ids])

    # Normalize vectors for cosine-like behavior with euclidean metric.
    # HDBSCAN's cosine metric can be finicky; normalizing + euclidean
    # gives equivalent clustering to cosine distance.
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    norms[norms == 0] = 1.0  # Avoid division by zero
    matrix = matrix / norms

    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=min_cluster_size,
        min_samples=min_samples,
        metric="euclidean",
    )
    labels = clusterer.fit_predict(matrix)

    clusters: dict[int, set[str]] = {}
    for pid, label in zip(ids, labels):
        if label == -1:
            continue  # Noise
        clusters.setdefault(label, set()).add(pid)

    return list(clusters.values())


def structural_clusters(
    refs: dict[str, set[str]],
) -> list[set[str]]:
    """Detect communities in the page reference graph using Leiden.

    Args:
        refs: {page_id: set of page_ids it references} -- adjacency list.

    Returns:
        List of sets, each set containing page IDs in a community.
        Only communities with 2+ members are returned.
    """
    if not refs:
        return []

    # Collect all page IDs (sources and targets)
    all_pages = set(refs.keys())
    for targets in refs.values():
        all_pages |= targets
    page_list = sorted(all_pages)
    idx = {pid: i for i, pid in enumerate(page_list)}

    edges = []
    for source, targets in refs.items():
        for target in targets:
            if source in idx and target in idx:
                edges.append((idx[source], idx[target]))

    if not edges:
        return []

    g = ig.Graph(n=len(page_list), edges=edges, directed=False)
    g.simplify()  # Remove duplicate edges and self-loops

    partition = leidenalg.find_partition(
        g, leidenalg.ModularityVertexPartition
    )

    clusters = []
    for community in partition:
        if len(community) >= 2:
            clusters.append({page_list[i] for i in community})

    return clusters


def merge_clusters(
    semantic: list[set[str]],
    structural: list[set[str]],
) -> list[set[str]]:
    """Merge semantic and structural clusters.

    Pages appearing in both a semantic and structural cluster together
    form high-confidence connections. All other clusters are preserved
    at lower confidence.

    Returns:
        List of page-ID sets. Overlapping clusters (pages in both
        semantic and structural) come first, followed by non-overlapping
        clusters from each source.
    """
    merged = []
    used_sem = set()
    used_struct = set()

    # Find overlapping clusters (high confidence)
    for si, sem_c in enumerate(semantic):
        for sti, struct_c in enumerate(structural):
            overlap = sem_c & struct_c
            if len(overlap) >= 2:
                merged.append(overlap)
                used_sem.add(si)
                used_struct.add(sti)

    # Add non-overlapping semantic clusters (medium confidence)
    for si, sem_c in enumerate(semantic):
        if si not in used_sem and len(sem_c) >= 2:
            merged.append(sem_c)

    # Add non-overlapping structural clusters (medium confidence)
    for sti, struct_c in enumerate(structural):
        if sti not in used_struct and len(struct_c) >= 2:
            merged.append(struct_c)

    return merged
