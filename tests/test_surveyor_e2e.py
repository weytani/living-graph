# ABOUTME: End-to-end integration tests for the surveyor pipeline.
# ABOUTME: Creates test pages in Roam, runs full pipeline, verifies structure and incremental behavior.

import json
import os
import time

import pytest

from living_graph.surveyor import SurveyorPipeline


@pytest.fixture
def e2e_pages(roam):
    """Create a cluster of related pages and one unrelated outlier.

    Three Python/testing pages that should cluster together,
    plus one cooking page that should not.  Cross-reference
    within the cluster to give the structural pass something to find.
    """
    cluster_a = [
        ("Test/E2E Python Dev", "Python development best practices and testing with pytest"),
        ("Test/E2E Python Testing", "Unit testing Python code using pytest and TDD methodology"),
        ("Test/E2E TDD Workflow", "Test-driven development workflow for Python projects"),
    ]
    outlier = [
        ("Test/E2E Cooking Italian", "Italian cooking recipes with fresh pasta and tomato sauce"),
    ]

    all_pages = cluster_a + outlier
    page_uids = {}

    # Create all pages first (before adding cross-references,
    # since [[refs]] auto-create pages and cause 400 on explicit create)
    for title, content in all_pages:
        roam.create_page(title)
        time.sleep(2)
        results = roam.q(
            f'[:find ?uid :where [?p :node/title "{title}"] [?p :block/uid ?uid]]'
        )
        page_uid = results[0][0]
        page_uids[title] = page_uid
        roam.create_block(page_uid, content, 0)
        time.sleep(1)

    # Now add cross-references (safe because all pages already exist)
    roam.create_block(
        page_uids["Test/E2E Python Dev"],
        "See also [[Test/E2E Python Testing]]",
        1,
    )
    time.sleep(1)

    return [p[0] for p in all_pages]


def test_e2e_full_pipeline(roam, claude, e2e_pages, tmp_path):
    """Full pipeline: embed pages, cluster, label, write — verify structural output."""
    pipeline = SurveyorPipeline(
        roam,
        claude,
        data_dir=str(tmp_path),
        run_prefix="Test/Run/",
        min_cluster_size=2,
    )
    result = pipeline.survey(page_titles=e2e_pages)

    # --- Result dict has expected keys ---
    assert "pages_embedded" in result
    assert "clusters_found" in result
    assert "relationships_written" in result
    assert "tags_written" in result
    assert "run_uid" in result

    # --- All pages were embedded ---
    assert result["pages_embedded"] == len(e2e_pages)

    # --- Run page was created ---
    assert result["run_uid"], "run_uid should be non-empty"
    time.sleep(2)
    tree = roam.pull(
        "[:block/uid :block/string {:block/children [:block/string]}]",
        f'[:block/uid "{result["run_uid"]}"]',
    )
    children = tree.get(":block/children", [])
    child_texts = [c.get(":block/string", "") for c in children]
    assert any("Process::" in t for t in child_texts), (
        f"Run page should have Process:: block, got: {child_texts}"
    )
    assert any("Status::" in t for t in child_texts), (
        f"Run page should have Status:: block, got: {child_texts}"
    )

    # --- State was persisted ---
    state_path = os.path.join(str(tmp_path), "surveyor_state.json")
    assert os.path.exists(state_path), "surveyor_state.json should exist"
    with open(state_path) as f:
        state = json.load(f)
    assert "page_hashes" in state
    assert len(state["page_hashes"]) == len(e2e_pages)


def test_e2e_incremental_embedding(roam, claude, e2e_pages, tmp_path):
    """Second run on unchanged pages should skip all embeddings."""
    pipeline = SurveyorPipeline(
        roam,
        claude,
        data_dir=str(tmp_path),
        run_prefix="Test/Run/",
        min_cluster_size=2,
    )

    # First run — embeds everything
    result1 = pipeline.survey(page_titles=e2e_pages)
    assert result1["pages_embedded"] == len(e2e_pages)

    # Second run — unchanged pages should be skipped.
    # Note: the first run may have written tags to some pages (changing
    # their content hash), so those pages will be re-embedded.  But at
    # least some pages should be skipped, proving incrementality works.
    result2 = pipeline.survey(page_titles=e2e_pages)
    assert result2["pages_embedded"] < result1["pages_embedded"], (
        f"Second run should embed fewer pages than first "
        f"({result2['pages_embedded']} >= {result1['pages_embedded']})"
    )
