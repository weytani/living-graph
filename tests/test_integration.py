# ABOUTME: End-to-end integration test exercising all deterministic layer modules.
# ABOUTME: Simulates a curator-like workflow: resolve, validate, interlink, log.

import time
from living_graph.entity_resolution import EntityResolver
from living_graph.validation import ValidationScanner
from living_graph.interlinking import Interlinker
from living_graph.mutation_log import MutationLogger


def test_full_curator_workflow(roam, test_namespace):
    """Simulate a curator workflow across all modules."""
    # 1. Start a run
    logger = MutationLogger(roam, namespace_prefix="Test/Run/")
    run = logger.create_run("IntegrationTest", "2026-02-24-e2e")

    # 2. Resolve an entity (should create since it doesn't exist)
    resolver = EntityResolver(roam)
    entity = resolver.resolve_or_create("Test/", "Integration Person")
    assert entity["created"] is True
    logger.log(run["uid"], "create", entity["title"], {"created": True})

    time.sleep(2)

    # 3. Add attributes so validation passes for Person type
    roam.create_block(entity["uid"], "Role:: Test Subject", 0)
    roam.create_block(entity["uid"], "Related:: [[Test/Integration Project]]", 1)

    # 4. Create a second entity to interlink with
    project = resolver.resolve_or_create("Test/", "Integration Project")
    logger.log(run["uid"], "create", project["title"], {"created": True})

    time.sleep(2)

    # 5. Validate the person page — should have no missing_attr issues
    scanner = ValidationScanner(roam)
    issues = scanner.validate_page("Test/Integration Person", type_name="Person")
    attr_issues = [i for i in issues if i.kind == "missing_attr"]
    assert len(attr_issues) == 0

    # 6. Interlink the two pages bidirectionally
    linker = Interlinker(roam)
    linker.link_bidirectional(
        entity["uid"], entity["title"],
        project["uid"], project["title"],
    )
    logger.log(
        run["uid"],
        "edit",
        entity["title"],
        {"added_related": project["title"]},
    )

    # 7. Close the run
    logger.close_run(run["uid"], "completed", "2 entities, 1 interlink")

    time.sleep(2)

    # 8. Verify the run page has all entries
    tree = roam.pull(
        "[:block/uid {:block/children [:block/string]}]",
        f'[:block/uid "{run["uid"]}"]',
    )
    children = tree.get(":block/children", [])
    strings = [c.get(":block/string", "") for c in children]
    assert any("Status:: completed" in s for s in strings)
    assert any("2 entities" in s for s in strings)
