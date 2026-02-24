# ABOUTME: Integration tests for the Roam API client.
# ABOUTME: Validates query, pull, write, and rate-limit handling against real graph.

import time


def test_query_returns_results(roam):
    """Basic Datalog query returns page titles."""
    results = roam.q("[:find ?title :where [?p :node/title ?title] :limit 5]")
    assert isinstance(results, list)
    assert len(results) > 0
    assert isinstance(results[0], list)


def test_pull_returns_entity(roam):
    """Pull a known page by title."""
    result = roam.pull("[*]", '[:node/title "Convention/Ontology"]')
    assert result is not None
    assert ":block/uid" in result or "block/uid" in result


def test_create_and_delete_page(roam):
    """Create a Test/ page, verify it exists, delete it."""
    roam.create_page("Test/Client Integration")
    time.sleep(2)
    results = roam.q(
        '[:find ?uid :where [?p :node/title "Test/Client Integration"] [?p :block/uid ?uid]]'
    )
    assert len(results) == 1
    uid = results[0][0]
    roam.delete_page(uid)
    time.sleep(2)
    results = roam.q(
        '[:find ?uid :where [?p :node/title "Test/Client Integration"] [?p :block/uid ?uid]]'
    )
    assert len(results) == 0


def test_create_and_delete_block(roam):
    """Create a block under a Test/ page, verify, cleanup."""
    roam.create_page("Test/Block Test")
    time.sleep(2)
    pages = roam.q(
        '[:find ?uid :where [?p :node/title "Test/Block Test"] [?p :block/uid ?uid]]'
    )
    page_uid = pages[0][0]

    roam.create_block(parent_uid=page_uid, string="test block content", order=0)
    time.sleep(2)
    children = roam.pull(
        "[:block/children {:block/children [:block/string :block/uid]}]",
        f'[:block/uid "{page_uid}"]',
    )
    child_key = ":block/children" if ":block/children" in children else "block/children"
    child_strings = [
        c.get(":block/string", c.get("block/string", ""))
        for c in children.get(child_key, [])
    ]
    assert "test block content" in child_strings

    roam.delete_page(page_uid)


def test_batch_actions(roam):
    """Batch multiple operations in one call."""
    roam.create_page("Test/Batch Test")
    time.sleep(2)
    pages = roam.q(
        '[:find ?uid :where [?p :node/title "Test/Batch Test"] [?p :block/uid ?uid]]'
    )
    page_uid = pages[0][0]

    roam.batch([
        {
            "action": "create-block",
            "location": {"parent-uid": page_uid, "order": 0},
            "block": {"string": "batch block 1"},
        },
        {
            "action": "create-block",
            "location": {"parent-uid": page_uid, "order": 1},
            "block": {"string": "batch block 2"},
        },
    ])

    time.sleep(2)
    children = roam.pull(
        "[:block/children {:block/children [:block/string]}]",
        f'[:block/uid "{page_uid}"]',
    )
    child_key = ":block/children" if ":block/children" in children else "block/children"
    strings = [
        c.get(":block/string", c.get("block/string", ""))
        for c in children.get(child_key, [])
    ]
    assert "batch block 1" in strings
    assert "batch block 2" in strings

    roam.delete_page(page_uid)
