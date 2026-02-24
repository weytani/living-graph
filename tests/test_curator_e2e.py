# ABOUTME: End-to-end integration test for the full curator pipeline.
# ABOUTME: Creates realistic content, runs all 4 stages, verifies the full output.

import time
from living_graph.curator import CuratorPipeline


def test_e2e_curator_creates_and_interlinks(roam, claude):
    """Full pipeline: realistic content → entity extraction → resolution → interlink → enrich."""
    # Create a page with content that should produce entities
    roam.create_page("Test/E2E Curator")
    time.sleep(2)
    pages = roam.q(
        '[:find ?uid :where [?p :node/title "Test/E2E Curator"] [?p :block/uid ?uid]]'
    )
    page_uid = pages[0][0]

    roam.create_block(
        page_uid,
        "Had a productive meeting with Sarah Kim, engineering lead at TechFlow, about migrating their data pipeline to a new architecture.",
        0,
    )
    roam.create_block(
        page_uid,
        "Reviewed the quarterly OKRs for the platform modernization project. Key blocker is the authentication refactor.",
        1,
    )
    time.sleep(2)

    # Run the curator
    pipeline = CuratorPipeline(roam, claude, run_prefix="Test/Run/")
    result = pipeline.curate_page("Test/E2E Curator")

    # Should have processed both blocks
    assert result["blocks_processed"] == 2
    assert result["run_uid"] is not None

    # Verify blocks are marked curated
    time.sleep(2)
    tree = roam.pull(
        "[:block/uid {:block/children [:block/string]}]",
        f'[:block/uid "{page_uid}"]',
    )
    children = tree.get(":block/children", [])

    # Find original source blocks by their unique content, excluding
    # structural blocks like Related:: that the interlinking stage adds.
    source_blocks = [
        c for c in children
        if not c.get(":block/string", "").startswith("Related::")
        and ("meeting" in c.get(":block/string", "").lower()
             or "quarterly" in c.get(":block/string", "").lower())
    ]
    assert len(source_blocks) == 2, (
        f"Expected 2 source blocks, found {len(source_blocks)}: "
        f"{[c.get(':block/string', '') for c in source_blocks]}"
    )
    for child in source_blocks:
        assert "#curated" in child.get(":block/string", ""), (
            f"Block not marked curated: {child.get(':block/string', '')}"
        )

    # Verify a Run/ page was created with entries
    run_tree = roam.pull(
        "[:block/uid {:block/children [:block/string]}]",
        f'[:block/uid "{result["run_uid"]}"]',
    )
    run_children = run_tree.get(":block/children", [])
    run_strings = [c.get(":block/string", "") for c in run_children]
    assert any("Status:: completed" in s for s in run_strings), (
        f"No 'Status:: completed' found in run log: {run_strings}"
    )
    assert any("analyze" in s.lower() for s in run_strings), (
        f"No 'analyze' entry found in run log: {run_strings}"
    )

    # If entities were extracted, verify they exist via the run log
    if result["entities_resolved"] > 0:
        assert any(
            "create" in s.lower() or "resolve" in s.lower() for s in run_strings
        ), f"No create/resolve entries found in run log: {run_strings}"
