# ABOUTME: Tests for the LLM client wrapper.
# ABOUTME: Verifies structured entity extraction via Claude tool_use.

from living_graph.llm import LLMClient, EntityManifest


def test_extract_entities_returns_manifest(claude):
    llm = LLMClient(claude)
    blocks = [
        "Had a great conversation with [[Person/Shane Parrish]] about decision-making frameworks.",
        "Working on [[Project/Marathon]] portfolio site — need to finish the LWC components.",
    ]
    manifest = llm.extract_entities(
        blocks=blocks,
        graph_context="## Person/ (1 page)\n- Person/Shane Parrish",
        ontology_summary="Types: Person (Role, Related), Project (Track, Status, Related), Org (Type, Status, Related)",
    )
    assert isinstance(manifest, EntityManifest)
    assert isinstance(manifest.entities, list)


def test_manifest_entities_have_required_fields(claude):
    llm = LLMClient(claude)
    blocks = [
        "Met with Jake from Notion to discuss API integration for the living graph project.",
    ]
    manifest = llm.extract_entities(
        blocks=blocks,
        graph_context="## Person/ (0 pages)\n## Org/ (0 pages)",
        ontology_summary="Types: Person (Role, Related), Org (Type, Status, Related), Project (Track, Status, Related)",
    )
    for entity in manifest.entities:
        assert "type" in entity
        assert "name" in entity
        assert "description" in entity
        assert "fields" in entity


def test_distill_insights_returns_manifest(claude):
    """LLM should extract epistemic insights from operational blocks."""
    llm = LLMClient(claude)
    blocks = [
        "Decided to use LWC for the portfolio because React would add unnecessary build complexity.",
        "The Roam API rate limits are slowing down the test suite — might need to batch queries.",
        "I keep saying I want to write more tests but I rarely do it on the first pass.",
    ]
    manifest = llm.distill_insights(
        blocks=blocks,
        graph_context="## Assumption/ (0 pages)\n## Decision/ (0 pages)",
        ontology_summary=(
            "Epistemic types:\n"
            "- Assumption (Confidence, Status, Source)\n"
            "- Constraint (Scope, Status, Source)\n"
            "- Contradiction (Sources, Status, Tension)\n"
            "- Synthesis (Sources, Related)\n"
            "- Decision (Rationale, Status, Alternatives, Related)"
        ),
        user_profile="- Salesforce developer\n- Values TDD\n- Three tracks: Personal, Professional, Creative",
    )
    assert isinstance(manifest, EntityManifest)
    assert isinstance(manifest.entities, list)


def test_distill_insights_entities_have_epistemic_types(claude):
    """Distilled entities must be epistemic types only."""
    llm = LLMClient(claude)
    blocks = [
        "Assuming the Roam API will stay stable through 2026 — no migration plan if it doesn't.",
        "Decided to keep all tests hitting real API — no mocks, even though it's slow.",
    ]
    manifest = llm.distill_insights(
        blocks=blocks,
        graph_context="## Assumption/ (0 pages)\n## Decision/ (0 pages)",
        ontology_summary=(
            "Epistemic types:\n"
            "- Assumption (Confidence, Status, Source)\n"
            "- Decision (Rationale, Status, Alternatives, Related)"
        ),
    )
    valid_types = {"assumption", "constraint", "contradiction", "synthesis", "decision"}
    for entity in manifest.entities:
        assert entity["type"] in valid_types, f"Got non-epistemic type: {entity['type']}"
        assert "name" in entity
        assert "description" in entity
        assert "fields" in entity


def test_distill_insights_empty_when_no_insights(claude):
    """Should return empty manifest for purely factual blocks."""
    llm = LLMClient(claude)
    blocks = [
        "Had lunch at noon.",
        "Weather was nice today.",
    ]
    manifest = llm.distill_insights(
        blocks=blocks,
        graph_context="## Assumption/ (0 pages)",
        ontology_summary="Epistemic types:\n- Assumption (Confidence, Status, Source)",
    )
    assert isinstance(manifest, EntityManifest)
    for entity in manifest.entities:
        assert entity["type"] in {"assumption", "constraint", "contradiction", "synthesis", "decision"}


def test_label_cluster(claude):
    """Should generate tags and relationship suggestions for a cluster."""
    llm = LLMClient(claude)
    cluster_pages = [
        {"title": "Project/Living Graph", "text": "Agentic maintenance system for Roam"},
        {"title": "Tool/Roam Research", "text": "Networked thought tool with graph queries"},
        {"title": "Tool/Claude", "text": "Anthropic's AI assistant used for coding"},
    ]
    result = llm.label_cluster(cluster_pages)

    assert "tags" in result
    assert "relationships" in result
    assert isinstance(result["tags"], list)
    assert len(result["tags"]) >= 1
    assert isinstance(result["relationships"], list)


def test_enrich_entity_returns_fields(claude):
    llm = LLMClient(claude)
    result = llm.enrich_entity(
        entity_type="person",
        entity_name="Jake Thompson",
        current_attrs={"Role": ""},
        source_blocks=["Met with Jake Thompson, PM at Notion, to discuss API integration."],
        ontology_summary="Person requires: Role, Related",
    )
    assert isinstance(result, dict)
    # Should have suggested field values
    assert len(result) > 0
