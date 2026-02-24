# ABOUTME: Tests for graph context builder.
# ABOUTME: Verifies compact snapshot of all typed pages is generated correctly.

import re
from living_graph.context import GraphContext


def test_build_context_returns_string(roam):
    ctx = GraphContext(roam)
    snapshot = ctx.build()
    assert isinstance(snapshot, str)
    assert len(snapshot) > 0


def test_context_groups_by_namespace(roam):
    ctx = GraphContext(roam)
    snapshot = ctx.build()
    # Should have section headers for populated namespaces
    assert "Person/" in snapshot
    assert "Project/" in snapshot


def test_context_lists_page_titles(roam):
    ctx = GraphContext(roam)
    snapshot = ctx.build()
    # Known pages from Phase 1 migration
    assert "Person/Shane Parrish" in snapshot


def test_context_includes_counts(roam):
    ctx = GraphContext(roam)
    snapshot = ctx.build()
    # Should show count per namespace
    counts = re.findall(r"\(\d+ pages?\)", snapshot)
    assert len(counts) > 0


def test_context_excludes_empty_namespaces(roam):
    ctx = GraphContext(roam)
    snapshot = ctx.build()
    lines = snapshot.strip().split("\n")
    for line in lines:
        if line.startswith("## ") and "(0 pages)" in line:
            assert False, f"Empty namespace should be excluded: {line}"
