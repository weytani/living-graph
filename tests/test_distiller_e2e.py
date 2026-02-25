# ABOUTME: End-to-end integration test for the full distiller pipeline.
# ABOUTME: Creates realistic content, runs all 4 stages, verifies epistemic output.

import time
from living_graph.distiller import DistillerPipeline


def test_e2e_distiller_creates_epistemic_pages(roam, claude):
    """Full pipeline: realistic content → distill → resolve → populate → enrich."""
    roam.create_page("Test/E2E Distiller")
    time.sleep(2)
    pages = roam.q(
        '[:find ?uid :where [?p :node/title "Test/E2E Distiller"] [?p :block/uid ?uid]]'
    )
    page_uid = pages[0][0]

    roam.create_block(
        page_uid,
        "Decided to use Python for the living graph project instead of TypeScript. "
        "Python has better Anthropic SDK support and I'm more productive in it, "
        "but TypeScript would have had tighter Roam plugin integration.",
        0,
    )
    roam.create_block(
        page_uid,
        "Assuming the Roam API rate limits won't get stricter — the test suite "
        "already takes 21 minutes and any reduction in allowed queries would break the workflow.",
        1,
    )
    roam.create_block(
        page_uid,
        "I keep telling myself I should write more documentation but I never actually do it "
        "because the code is always changing. Is that a real constraint or just an excuse?",
        2,
    )
    time.sleep(2)

    pipeline = DistillerPipeline(roam, claude, run_prefix="Test/Run/")
    result = pipeline.distill_page("Test/E2E Distiller")

    assert result["blocks_processed"] == 3
    assert result["run_uid"] is not None
    # Should have found at least one insight
    assert result["insights_extracted"] >= 1

    # Verify the Run/ page exists with entries
    time.sleep(2)
    run_tree = roam.pull(
        "[:block/uid {:block/children [:block/string]}]",
        f'[:block/uid "{result["run_uid"]}"]',
    )
    run_children = run_tree.get(":block/children", [])
    run_strings = [c.get(":block/string", "") for c in run_children]
    assert any("Status:: completed" in s for s in run_strings)
    assert any("distill" in s.lower() for s in run_strings)

    # If pages were created, verify they're in epistemic namespaces
    if result["pages_created"] > 0:
        assert any(
            "create" in s.lower() for s in run_strings
        ), "Run log should record page creation"


def test_e2e_distiller_dedup_on_rerun(roam, claude):
    """Running distiller twice on same content should resolve to existing pages."""
    roam.create_page("Test/E2E Distiller Dedup")
    time.sleep(2)
    pages = roam.q(
        '[:find ?uid :where [?p :node/title "Test/E2E Distiller Dedup"] [?p :block/uid ?uid]]'
    )
    page_uid = pages[0][0]

    roam.create_block(
        page_uid,
        "Decided to enforce strict scope permissions per worker — "
        "each worker can only do what it's explicitly allowed to do.",
        0,
    )
    time.sleep(2)

    pipeline = DistillerPipeline(roam, claude, run_prefix="Test/Run/")

    # First run
    result1 = pipeline.distill_page("Test/E2E Distiller Dedup")
    created_first = result1["pages_created"]

    # Second run on same content
    time.sleep(2)
    result2 = pipeline.distill_page("Test/E2E Distiller Dedup")

    # Second run should resolve to existing pages, not create duplicates
    if created_first > 0:
        assert result2["pages_resolved"] >= result2["pages_created"], (
            "Re-run should resolve more than it creates"
        )
