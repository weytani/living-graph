# ABOUTME: LLM client for living graph workers — wraps Anthropic Claude API.
# ABOUTME: Uses tool_use for structured entity extraction and enrichment.

from __future__ import annotations

from dataclasses import dataclass, field


ENTITY_TOOL = {
    "name": "extract_entities",
    "description": "Extract entities and relationships from daily page blocks.",
    "input_schema": {
        "type": "object",
        "properties": {
            "entities": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "type": {
                            "type": "string",
                            "description": "The entity type (lowercase): person, org, project, tool, etc.",
                        },
                        "name": {
                            "type": "string",
                            "description": "The entity name in Title Case.",
                        },
                        "description": {
                            "type": "string",
                            "description": "1-2 sentence description of the entity from context.",
                        },
                        "fields": {
                            "type": "object",
                            "description": "Attribute values to set on the entity page. Keys are attribute names (Role, Status, etc.), values are strings.",
                        },
                    },
                    "required": ["type", "name", "description", "fields"],
                },
            },
        },
        "required": ["entities"],
    },
}

ENRICH_TOOL = {
    "name": "enrich_entity",
    "description": "Suggest attribute values for an entity page based on source context.",
    "input_schema": {
        "type": "object",
        "properties": {
            "fields": {
                "type": "object",
                "description": "Attribute names and their suggested values. Only include fields that have meaningful values from the source context.",
            },
        },
        "required": ["fields"],
    },
}


@dataclass
class EntityManifest:
    """Structured output from the LLM entity extraction."""
    entities: list[dict] = field(default_factory=list)


class LLMClient:
    """Wraps the Anthropic API for structured entity operations."""

    MODEL = "claude-sonnet-4-20250514"

    def __init__(self, client, model: str | None = None):
        self._client = client
        self._model = model or self.MODEL

    def extract_entities(
        self,
        blocks: list[str],
        graph_context: str,
        ontology_summary: str,
    ) -> EntityManifest:
        """Extract entities from daily page blocks using tool_use.

        Args:
            blocks: List of block text strings to analyze.
            graph_context: Compact snapshot of existing graph pages.
            ontology_summary: Summary of available types and required fields.

        Returns:
            EntityManifest with extracted entities.
        """
        blocks_text = "\n".join(f"- {b}" for b in blocks)

        system = (
            "You are a curator for a personal knowledge graph. "
            "Your job is to identify entities (people, organizations, projects, tools, etc.) "
            "mentioned in daily page blocks and extract them as structured data.\n\n"
            "RULES:\n"
            "- Only extract entities that are meaningful to the graph owner's life and work.\n"
            "- Skip media references, historical figures mentioned in passing, generic concepts.\n"
            "- Check the existing graph context to avoid extracting entities that already exist.\n"
            "- For existing entities, only include them if the source adds NEW information.\n"
            "- Use the ontology to determine the correct type and required fields.\n"
            "- Name entities in Title Case.\n"
            "- The 'description' field should be 1-2 sentences of context from the source.\n"
            "- The 'fields' dict should include values for the type's required attributes "
            "where the source provides enough information.\n"
            "- For Related:: fields, use [[Namespace/Name]] format.\n"
            "- If no entities are worth extracting, return an empty entities array.\n"
        )

        user = (
            f"## Ontology\n{ontology_summary}\n\n"
            f"## Existing Graph\n{graph_context}\n\n"
            f"## Daily Page Blocks to Analyze\n{blocks_text}\n\n"
            "Extract any meaningful entities from these blocks."
        )

        response = self._client.messages.create(
            model=self._model,
            max_tokens=4096,
            system=system,
            tools=[ENTITY_TOOL],
            tool_choice={"type": "tool", "name": "extract_entities"},
            messages=[{"role": "user", "content": user}],
        )

        # Extract the tool_use result
        for block in response.content:
            if block.type == "tool_use" and block.name == "extract_entities":
                return EntityManifest(entities=block.input.get("entities", []))

        return EntityManifest()

    def enrich_entity(
        self,
        entity_type: str,
        entity_name: str,
        current_attrs: dict[str, str],
        source_blocks: list[str],
        ontology_summary: str,
    ) -> dict:
        """Suggest attribute values for an entity based on source context.

        Returns dict of {attr_name: suggested_value}.
        """
        blocks_text = "\n".join(f"- {b}" for b in source_blocks)
        current = "\n".join(f"  {k}:: {v}" for k, v in current_attrs.items())

        system = (
            "You are enriching an entity page in a personal knowledge graph. "
            "Based on the source material, suggest values for empty or missing attributes. "
            "Only suggest values you can confidently derive from the source. "
            "Do not guess or fabricate information. "
            "If nothing can be added, return an empty fields object."
        )

        user = (
            f"## Entity\n"
            f"Type: {entity_type}\n"
            f"Name: {entity_name}\n"
            f"Current attributes:\n{current}\n\n"
            f"## Ontology\n{ontology_summary}\n\n"
            f"## Source Material\n{blocks_text}\n\n"
            "Suggest attribute values based on the source material."
        )

        response = self._client.messages.create(
            model=self._model,
            max_tokens=1024,
            system=system,
            tools=[ENRICH_TOOL],
            tool_choice={"type": "tool", "name": "enrich_entity"},
            messages=[{"role": "user", "content": user}],
        )

        for block in response.content:
            if block.type == "tool_use" and block.name == "enrich_entity":
                return block.input.get("fields", {})

        return {}
