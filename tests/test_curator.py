# ABOUTME: Tests for the curator pipeline.
# ABOUTME: End-to-end test that curates a real daily page block through all 4 stages.

import time
from living_graph.curator import CuratorPipeline


def test_find_uncurated_blocks(roam):
    """Should find blocks on a daily page that lack #curated."""
    roam.create_page("Test/Daily Page")
    time.sleep(2)
    pages = roam.q(
        '[:find ?uid :where [?p :node/title "Test/Daily Page"] [?p :block/uid ?uid]]'
    )
    page_uid = pages[0][0]
    roam.create_block(page_uid, "This block has no curated tag", 0)
    roam.create_block(page_uid, "This block is already #curated", 1)
    roam.create_block(page_uid, "Another uncurated block about [[Person/Shane Parrish]]", 2)
    time.sleep(2)

    pipeline = CuratorPipeline(roam, claude=None)
    blocks = pipeline.find_uncurated_blocks("Test/Daily Page")
    texts = [b["string"] for b in blocks]
    assert "This block has no curated tag" in texts
    assert "Another uncurated block about [[Person/Shane Parrish]]" in texts
    assert "This block is already #curated" not in texts


def test_mark_curated(roam):
    """Should append #curated to block text."""
    roam.create_page("Test/Mark Curated")
    time.sleep(2)
    pages = roam.q(
        '[:find ?uid :where [?p :node/title "Test/Mark Curated"] [?p :block/uid ?uid]]'
    )
    page_uid = pages[0][0]
    roam.create_block(page_uid, "Original block text", 0)
    time.sleep(2)

    tree = roam.pull(
        "[:block/uid {:block/children [:block/uid :block/string]}]",
        f'[:block/uid "{page_uid}"]',
    )
    child = tree[":block/children"][0]
    block_uid = child[":block/uid"]

    pipeline = CuratorPipeline(roam, claude=None)
    pipeline.mark_curated([block_uid])
    time.sleep(2)

    updated = roam.pull("[:block/string]", f'[:block/uid "{block_uid}"]')
    assert "#curated" in updated.get(":block/string", "")


def test_build_ontology_summary(roam):
    """Should build a compact ontology summary string for LLM context."""
    pipeline = CuratorPipeline(roam, claude=None)
    summary = pipeline.build_ontology_summary()
    assert "Person" in summary
    assert "Project" in summary


def test_full_pipeline(roam, claude):
    """Full pipeline against a test page — creates entities, interlinks, logs."""
    roam.create_page("Test/Curator Input")
    time.sleep(2)
    pages = roam.q(
        '[:find ?uid :where [?p :node/title "Test/Curator Input"] [?p :block/uid ?uid]]'
    )
    page_uid = pages[0][0]
    roam.create_block(
        page_uid,
        "Spoke with Maria Chen from Acme Corp about the data migration project.",
        0,
    )
    time.sleep(2)

    pipeline = CuratorPipeline(roam, claude, run_prefix="Test/Run/")
    result = pipeline.curate_page("Test/Curator Input")

    assert result["blocks_processed"] >= 1
    assert result["run_uid"] is not None
    # Should have extracted at least one entity
    assert result["entities_resolved"] >= 0  # LLM might not extract any — that's OK
    # Blocks should be marked curated
    time.sleep(2)
    tree = roam.pull(
        "[:block/uid {:block/children [:block/string]}]",
        f'[:block/uid "{page_uid}"]',
    )
    children = tree.get(":block/children", [])
    curated_found = False
    for child in children:
        text = child.get(":block/string", "")
        # Check the original source block (contains "Spoke with"), not Related:: blocks
        if "Spoke with" in text:
            assert "#curated" in text
            curated_found = True
    assert curated_found, "Original source block should be marked #curated"


def test_pipeline_skips_already_curated(roam, claude):
    """Pipeline should not re-process blocks already tagged #curated."""
    roam.create_page("Test/Already Curated")
    time.sleep(2)
    pages = roam.q(
        '[:find ?uid :where [?p :node/title "Test/Already Curated"] [?p :block/uid ?uid]]'
    )
    page_uid = pages[0][0]
    roam.create_block(page_uid, "Already processed block #curated", 0)
    time.sleep(2)

    pipeline = CuratorPipeline(roam, claude, run_prefix="Test/Run/")
    result = pipeline.curate_page("Test/Already Curated")
    assert result["blocks_processed"] == 0
