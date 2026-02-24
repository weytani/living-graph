# ABOUTME: Tests for mutation logging.
# ABOUTME: Verifies Run/ pages are created with structured mutation entries.

import time
from living_graph.mutation_log import MutationLogger


def test_create_run(roam, test_namespace):
    logger = MutationLogger(roam, namespace_prefix="Test/Run/")
    run = logger.create_run("TestWorker", "2026-02-24")
    assert run["title"] == "Test/Run/TestWorker 2026-02-24"
    assert "uid" in run


def test_log_mutation(roam, test_namespace):
    logger = MutationLogger(roam, namespace_prefix="Test/Run/")
    run = logger.create_run("TestWorker", "2026-02-24-log")
    logger.log(
        run_uid=run["uid"],
        action="create",
        target="Test/Some Page",
        changes={"title": "Test/Some Page", "attrs": ["Role::", "Related::"]},
    )
    time.sleep(2)
    tree = roam.pull(
        "[:block/uid {:block/children [:block/string]}]",
        f'[:block/uid "{run["uid"]}"]',
    )
    children = tree.get(":block/children", [])
    strings = [c.get(":block/string", "") for c in children]
    mutation_blocks = [s for s in strings if "create" in s.lower()]
    assert len(mutation_blocks) >= 1


def test_close_run(roam, test_namespace):
    logger = MutationLogger(roam, namespace_prefix="Test/Run/")
    run = logger.create_run("TestWorker", "2026-02-24-close")
    logger.close_run(run["uid"], status="completed", summary="3 pages created")
    time.sleep(2)
    tree = roam.pull(
        "[:block/uid {:block/children [:block/string]}]",
        f'[:block/uid "{run["uid"]}"]',
    )
    children = tree.get(":block/children", [])
    strings = [c.get(":block/string", "") for c in children]
    assert any("Status:: completed" in s for s in strings)
    assert any("3 pages created" in s for s in strings)


def test_multiple_mutations_in_run(roam, test_namespace):
    logger = MutationLogger(roam, namespace_prefix="Test/Run/")
    run = logger.create_run("TestWorker", "2026-02-24-multi")
    logger.log(run["uid"], "create", "Test/Page A", {"title": "Test/Page A"})
    logger.log(run["uid"], "edit", "Test/Page B", {"attr": "Status", "old": "active", "new": "paused"})
    logger.log(run["uid"], "delete", "Test/Page C", {})
    time.sleep(2)
    tree = roam.pull(
        "[:block/uid {:block/children [:block/string]}]",
        f'[:block/uid "{run["uid"]}"]',
    )
    children = tree.get(":block/children", [])
    strings = [c.get(":block/string", "") for c in children]
    mutation_blocks = [s for s in strings if any(
        op in s.lower() for op in ["create", "edit", "delete"]
    )]
    assert len(mutation_blocks) >= 3
