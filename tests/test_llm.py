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
