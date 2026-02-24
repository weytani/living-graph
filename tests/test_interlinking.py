# ABOUTME: Tests for interlinking helpers.
# ABOUTME: Verifies bidirectional Related:: wiring between Test/ namespace pages.

import time
from living_graph.interlinking import Interlinker


def test_find_unlinked_references(roam, test_namespace):
    roam.create_page("Test/Link Target")
    roam.create_page("Test/Link Source")
    time.sleep(2)
    pages = roam.q(
        '[:find ?uid :where [?p :node/title "Test/Link Source"] [?p :block/uid ?uid]]'
    )
    source_uid = pages[0][0]
    roam.create_block(source_uid, "Mentions [[Test/Link Target]] in passing", 0)
    time.sleep(2)
    linker = Interlinker(roam)
    unlinked = linker.find_unlinked_references("Test/Link Target")
    assert any("Test/Link Source" in ref["title"] for ref in unlinked)


def test_add_related(roam, test_namespace):
    roam.create_page("Test/Relate A")
    time.sleep(2)
    pages_a = roam.q(
        '[:find ?uid :where [?p :node/title "Test/Relate A"] [?p :block/uid ?uid]]'
    )
    uid_a = pages_a[0][0]
    linker = Interlinker(roam)
    linker.add_related(uid_a, ["Test/Relate B"])
    time.sleep(2)
    tree = roam.pull(
        "[:block/uid {:block/children [:block/string]}]",
        f'[:block/uid "{uid_a}"]',
    )
    children = tree.get(":block/children", [])
    strings = [c.get(":block/string", "") for c in children]
    assert any("Related::" in s and "Test/Relate B" in s for s in strings)


def test_add_related_appends_to_existing(roam, test_namespace):
    roam.create_page("Test/Append Related")
    time.sleep(2)
    pages = roam.q(
        '[:find ?uid :where [?p :node/title "Test/Append Related"] [?p :block/uid ?uid]]'
    )
    page_uid = pages[0][0]
    roam.create_block(page_uid, "Related:: [[Test/Existing Link]]", 0)
    time.sleep(2)
    linker = Interlinker(roam)
    linker.add_related(page_uid, ["Test/New Link"])
    time.sleep(2)
    tree = roam.pull(
        "[:block/uid {:block/children [:block/string]}]",
        f'[:block/uid "{page_uid}"]',
    )
    children = tree.get(":block/children", [])
    strings = [c.get(":block/string", "") for c in children]
    related = [s for s in strings if s.startswith("Related::")]
    assert len(related) == 1  # Should NOT create a second Related:: block
    assert "Test/Existing Link" in related[0]
    assert "Test/New Link" in related[0]


def test_bidirectional_link(roam, test_namespace):
    roam.create_page("Test/Bi A")
    roam.create_page("Test/Bi B")
    time.sleep(2)
    pages_a = roam.q(
        '[:find ?uid :where [?p :node/title "Test/Bi A"] [?p :block/uid ?uid]]'
    )
    pages_b = roam.q(
        '[:find ?uid :where [?p :node/title "Test/Bi B"] [?p :block/uid ?uid]]'
    )
    uid_a = pages_a[0][0]
    uid_b = pages_b[0][0]
    linker = Interlinker(roam)
    linker.link_bidirectional(uid_a, "Test/Bi A", uid_b, "Test/Bi B")
    time.sleep(2)
    tree_a = roam.pull(
        "[:block/uid {:block/children [:block/string]}]",
        f'[:block/uid "{uid_a}"]',
    )
    strings_a = [c.get(":block/string", "") for c in tree_a.get(":block/children", [])]
    assert any("Test/Bi B" in s for s in strings_a)
    tree_b = roam.pull(
        "[:block/uid {:block/children [:block/string]}]",
        f'[:block/uid "{uid_b}"]',
    )
    strings_b = [c.get(":block/string", "") for c in tree_b.get(":block/children", [])]
    assert any("Test/Bi A" in s for s in strings_b)
