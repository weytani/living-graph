# ABOUTME: LLM client for living graph workers — wraps Anthropic Claude API.
# ABOUTME: Uses tool_use for structured entity extraction, enrichment, and autofix.

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


DISTILL_TOOL = {
    "name": "distill_insights",
    "description": "Extract implicit epistemic knowledge from daily page blocks.",
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
                            "enum": [
                                "assumption", "constraint", "contradiction",
                                "synthesis", "decision",
                            ],
                            "description": "The epistemic type.",
                        },
                        "name": {
                            "type": "string",
                            "description": "Short descriptive name in Title Case (e.g. 'Roam API Stability').",
                        },
                        "description": {
                            "type": "string",
                            "description": "1-3 sentence explanation of the insight and its context.",
                        },
                        "fields": {
                            "type": "object",
                            "description": (
                                "Attribute values per the ontology. Keys depend on type: "
                                "Assumption→{Confidence,Status,Source}, "
                                "Constraint→{Scope,Status,Source}, "
                                "Contradiction→{Sources,Status,Tension}, "
                                "Synthesis→{Sources,Related}, "
                                "Decision→{Rationale,Status,Alternatives,Related}."
                            ),
                        },
                    },
                    "required": ["type", "name", "description", "fields"],
                },
            },
        },
        "required": ["entities"],
    },
}

LABEL_CLUSTER_TOOL = {
    "name": "label_cluster",
    "description": "Label a cluster of related pages with hierarchical tags and suggest relationships.",
    "input_schema": {
        "type": "object",
        "properties": {
            "tags": {
                "type": "array",
                "items": {"type": "string"},
                "description": (
                    "1-3 hierarchical tags for this cluster. "
                    "Use slash-separated hierarchy (e.g. 'knowledge-management/personal-tools')."
                ),
            },
            "relationships": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "source": {"type": "string", "description": "Page title of the source."},
                        "target": {"type": "string", "description": "Page title of the target."},
                        "type": {
                            "type": "string",
                            "enum": [
                                "related-to", "supports", "depends-on",
                                "part-of", "supersedes", "contradicts",
                            ],
                            "description": "The relationship type.",
                        },
                        "reasoning": {"type": "string", "description": "Why this relationship exists."},
                    },
                    "required": ["source", "target", "type", "reasoning"],
                },
                "description": "Suggested relationships between pages in the cluster.",
            },
        },
        "required": ["tags", "relationships"],
    },
}

AUTOFIX_TOOL = {
    "name": "suggest_fix",
    "description": "Suggest a fix for a validation issue on a Roam knowledge graph page.",
    "input_schema": {
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": ["edit_block", "delete_block", "delete_page", "skip"],
                "description": (
                    "The fix to apply. edit_block: update a block's content. "
                    "delete_block: remove a specific block. "
                    "delete_page: remove the entire page. "
                    "skip: no fix needed or issue is informational."
                ),
            },
            "target_uid": {
                "type": "string",
                "description": "UID of the block to edit or delete. Required for edit_block and delete_block.",
            },
            "new_value": {
                "type": "string",
                "description": "New block content for edit_block action.",
            },
            "reasoning": {
                "type": "string",
                "description": "Brief explanation of why this fix is appropriate.",
            },
        },
        "required": ["action", "reasoning"],
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
        user_profile: str = "",
    ) -> EntityManifest:
        """Extract entities from daily page blocks using tool_use.

        Args:
            blocks: List of block text strings to analyze.
            graph_context: Compact snapshot of existing graph pages.
            ontology_summary: Summary of available types and required fields.
            user_profile: Owner profile for relevance filtering.

        Returns:
            EntityManifest with extracted entities.
        """
        blocks_text = "\n".join(f"- {b}" for b in blocks)

        profile_section = ""
        if user_profile:
            profile_section = (
                "\n\nGRAPH OWNER PROFILE (use this to judge relevance):\n"
                + user_profile + "\n"
            )

        system = (
            "You are a curator for a personal knowledge graph. "
            "Your job is to identify entities (people, organizations, projects, tools, etc.) "
            "mentioned in daily page blocks and extract them as structured data.\n\n"
            "RULES:\n"
            "- Only extract entities the graph owner directly interacts with or works on.\n"
            "- Skip: media references, historical figures, celebrities mentioned in passing, "
            "generic concepts, subjects of analysis (vs subjects of work).\n"
            "- Check the existing graph context to avoid extracting entities that already exist.\n"
            "- For existing entities, only include them if the source adds NEW information.\n"
            "- Use the ontology to determine the correct type and required fields.\n"
            "- Name entities in Title Case.\n"
            "- The 'description' field should be 1-2 sentences of context from the source.\n"
            "- The 'fields' dict should include values for the type's required attributes "
            "where the source provides enough information.\n"
            "- For Related:: fields, use [[Namespace/Name]] format.\n"
            "- If no entities are worth extracting, return an empty entities array.\n"
            + profile_section
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

    def label_cluster(self, cluster_pages: list[dict]) -> dict:
        """Label a cluster of pages with tags and suggest relationships.

        Args:
            cluster_pages: List of {"title": str, "text": str} dicts.

        Returns:
            Dict with "tags" (list of str) and "relationships" (list of dicts).
        """
        pages_text = "\n\n".join(
            f"### {p['title']}\n{p['text']}" for p in cluster_pages
        )

        system = (
            "You are analyzing a cluster of semantically related pages in a personal knowledge graph. "
            "Your job is to:\n"
            "1. Assign 1-3 hierarchical tags that describe what this cluster is about.\n"
            "2. Suggest specific relationships between pages in the cluster.\n\n"
            "RELATIONSHIP TYPES:\n"
            "- related-to: General thematic connection\n"
            "- supports: Source provides evidence/foundation for target\n"
            "- depends-on: Source requires target to function\n"
            "- part-of: Source is a component of target\n"
            "- supersedes: Source replaces or updates target\n"
            "- contradicts: Source conflicts with target\n\n"
            "RULES:\n"
            "- Tags should be lowercase, slash-separated hierarchy (e.g. 'ai/llm-tools')\n"
            "- Only suggest relationships with clear evidence from the page content\n"
            "- Prefer specific relationship types over generic 'related-to'\n"
            "- Use exact page titles as they appear\n"
        )

        user = f"## Cluster Pages\n{pages_text}\n\nLabel this cluster and suggest relationships."

        response = self._client.messages.create(
            model=self._model,
            max_tokens=2048,
            system=system,
            tools=[LABEL_CLUSTER_TOOL],
            tool_choice={"type": "tool", "name": "label_cluster"},
            messages=[{"role": "user", "content": user}],
        )

        for block in response.content:
            if block.type == "tool_use" and block.name == "label_cluster":
                return block.input

        return {"tags": [], "relationships": []}

    def distill_insights(
        self,
        blocks: list[str],
        graph_context: str,
        ontology_summary: str,
        user_profile: str = "",
    ) -> EntityManifest:
        """Extract implicit epistemic knowledge from daily page blocks.

        Surfaces assumptions, decisions, constraints, contradictions, and
        syntheses that are implied but not explicitly stated.

        Args:
            blocks: List of block text strings to analyze.
            graph_context: Compact snapshot of existing epistemic pages.
            ontology_summary: Summary of epistemic types and their attributes.
            user_profile: Owner profile for relevance filtering.

        Returns:
            EntityManifest with epistemic entities.
        """
        blocks_text = "\n".join(f"- {b}" for b in blocks)

        profile_section = ""
        if user_profile:
            profile_section = (
                "\n\nGRAPH OWNER PROFILE (use for relevance filtering):\n"
                + user_profile + "\n"
            )

        system = (
            "You are a distiller for a personal knowledge graph. "
            "Your job is to surface IMPLICIT epistemic knowledge from daily page blocks — "
            "things the author believes, decided, or is constrained by, but didn't explicitly state.\n\n"
            "TYPES YOU CAN EXTRACT:\n"
            "- **assumption**: An unstated belief the author is operating on. "
            "Something they take for granted that could be wrong.\n"
            "- **decision**: A choice that was made (explicitly or implicitly) with alternatives not chosen. "
            "Not a task — a fork in the road where one path was taken.\n"
            "- **constraint**: A known boundary or limitation being worked within or around.\n"
            "- **contradiction**: A tension between two stated positions or between stated intent and action.\n"
            "- **synthesis**: An integrated learning that resolves or transcends earlier assumptions/contradictions.\n\n"
            "RULES:\n"
            "- Only extract insights genuinely present in the source. Do not fabricate.\n"
            "- Check existing epistemic pages to avoid duplicates.\n"
            "- Name insights descriptively in Title Case (e.g. 'Roam API Rate Limit Impact').\n"
            "- The 'description' should explain the insight in 1-3 sentences.\n"
            "- Set 'fields' per the ontology requirements for each type.\n"
            "- For Source:: fields, reference the daily page title in [[brackets]].\n"
            "- For Confidence:: use high/medium/low.\n"
            "- For Status:: use the first valid status (active, unresolved, etc.).\n"
            "- Prefer fewer, higher-quality insights over many weak ones.\n"
            "- If no epistemic insights are worth extracting, return an empty array.\n"
            + profile_section
        )

        user = (
            f"## Epistemic Ontology\n{ontology_summary}\n\n"
            f"## Existing Epistemic Graph\n{graph_context}\n\n"
            f"## Daily Page Blocks to Analyze\n{blocks_text}\n\n"
            "Surface any implicit assumptions, decisions, constraints, contradictions, "
            "or syntheses from these blocks."
        )

        response = self._client.messages.create(
            model=self._model,
            max_tokens=4096,
            system=system,
            tools=[DISTILL_TOOL],
            tool_choice={"type": "tool", "name": "distill_insights"},
            messages=[{"role": "user", "content": user}],
        )

        for block in response.content:
            if block.type == "tool_use" and block.name == "distill_insights":
                return EntityManifest(entities=block.input.get("entities", []))

        return EntityManifest()

    def suggest_fix(
        self,
        issue,
        page_title: str,
        page_context: str,
        ontology_summary: str,
    ) -> dict:
        """Suggest a fix for a validation issue.

        Args:
            issue: A validation Issue object with kind, severity, detail.
            page_title: Title of the page with the issue.
            page_context: Block listing with UIDs: "[uid] block text" per line.
            ontology_summary: Summary of available types and required fields.

        Returns:
            Dict with keys: action, target_uid, new_value, reasoning.
        """
        system = (
            "You are a janitor for a personal knowledge graph maintained in Roam Research. "
            "Your job is to fix validation issues found by the scanner.\n\n"
            "HARD CONSTRAINTS:\n"
            "- You can ONLY edit or delete. You CANNOT create new pages.\n"
            "- For edit_block: provide the target_uid and the complete new block text.\n"
            "- For delete_block: provide the target_uid of the block to remove.\n"
            "- For delete_page: no target_uid needed — the entire page will be removed.\n"
            "- Use 'skip' when the issue is informational or cannot be fixed by editing.\n\n"
            "FIX GUIDELINES:\n"
            "- missing_attr: Skip — janitor cannot create blocks (would need create permission).\n"
            "- invalid_status: Edit the Status:: block to the closest valid status value.\n"
            "- stub: Delete the page if it has no useful content.\n"
            "- broken_link: Edit the Related:: block to remove the bare text (deleted page reference).\n"
            "- orphan: Skip — orphan status is informational and may resolve naturally.\n"
        )

        user = (
            f"## Ontology\n{ontology_summary}\n\n"
            f"## Page: {page_title}\n"
            f"## Page Blocks (format: [uid] text)\n{page_context}\n\n"
            f"## Issue\n"
            f"Kind: {issue.kind}\n"
            f"Severity: {issue.severity}\n"
            f"Detail: {issue.detail}\n\n"
            "Suggest a fix for this issue."
        )

        response = self._client.messages.create(
            model=self._model,
            max_tokens=1024,
            system=system,
            tools=[AUTOFIX_TOOL],
            tool_choice={"type": "tool", "name": "suggest_fix"},
            messages=[{"role": "user", "content": user}],
        )

        for block in response.content:
            if block.type == "tool_use" and block.name == "suggest_fix":
                return block.input

        return {"action": "skip", "reasoning": "No fix suggested by LLM"}

    def repair_link(
        self,
        page_title: str,
        bare_text: str,
        candidates: list[str],
        block_uid: str,
        current_value: str,
    ) -> dict:
        """Resolve an ambiguous broken link by choosing among candidates.

        Args:
            page_title: Page containing the broken link.
            bare_text: The bare text (deleted page remnant) in Related::.
            candidates: List of candidate page titles that might match.
            block_uid: UID of the Related:: block.
            current_value: Current full text of the Related:: block.

        Returns:
            Dict with keys: action, target_uid, new_value, reasoning.
        """
        candidates_text = "\n".join(
            f"  - [[{c}]]" for c in candidates[:15]
        )

        system = (
            "You are a janitor for a personal knowledge graph in Roam Research. "
            "A page's Related:: attribute contains bare text where a wikilink used to be "
            "(Roam strips [[brackets]] when the referenced page is deleted). "
            "You have candidate pages that might be the correct replacement.\n\n"
            "RULES:\n"
            "- If one candidate clearly matches the bare text, fix the link.\n"
            "- Replace the bare text with [[Candidate/Name]] in the Related:: value.\n"
            "- If no candidate is a clear match, use action 'skip'.\n"
            "- Preserve all existing valid [[links]] in the Related:: value.\n"
            "- Do NOT create new pages — only fix the link text.\n"
        )

        user = (
            f"## Page: {page_title}\n"
            f"## Related:: block (UID: {block_uid})\n"
            f"Current value: {current_value}\n"
            f"Bare text to fix: '{bare_text}'\n\n"
            f"## Candidate Pages\n{candidates_text}\n\n"
            "Which candidate should replace the bare text?"
        )

        response = self._client.messages.create(
            model=self._model,
            max_tokens=1024,
            system=system,
            tools=[AUTOFIX_TOOL],
            tool_choice={"type": "tool", "name": "suggest_fix"},
            messages=[{"role": "user", "content": user}],
        )

        for block in response.content:
            if block.type == "tool_use" and block.name == "suggest_fix":
                return block.input

        return {"action": "skip", "reasoning": "Could not resolve ambiguous link"}

    def enrich_stub(
        self,
        page_title: str,
        type_name: str,
        required_attrs: list[str],
        valid_statuses: list[str],
        linked_context: str,
        ontology_summary: str,
    ) -> dict:
        """Enrich a stub page using linked context. No fabrication.

        Args:
            page_title: Title of the stub page to enrich.
            type_name: The entity type (Person, Project, etc.).
            required_attrs: List of required attribute names.
            valid_statuses: List of valid status values (empty if type has none).
            linked_context: Formatted text of pages that link to this stub.
            ontology_summary: Summary of the ontology for type reference.

        Returns:
            Dict with: action ("enrich"|"delete"|"skip"), fields, reasoning.
        """
        req_text = ", ".join(required_attrs) if required_attrs else "none"
        status_text = ", ".join(valid_statuses) if valid_statuses else "none"

        system = (
            "You are a janitor enriching a stub record in a personal knowledge graph (Roam Research). "
            "The page exists but has no content. Your job is to fill in attributes using ONLY "
            "information from linked pages that reference this entity.\n\n"
            "HARD RULES:\n"
            "- Only use facts from the linked context below. Never invent information.\n"
            "- For Person/Org types, you may include verifiable public facts (role, company).\n"
            "- For all other types, use ONLY vault context.\n"
            "- If there is insufficient context to add ANY meaningful attributes, return action 'skip'.\n"
            "- If the page appears to be garbage (test data, nonsense), return action 'delete'.\n"
            "- Use [[Namespace/Name]] format for Related:: values.\n"
            "- For Status:: values, use only from the valid list.\n"
        )

        user = (
            f"## Stub Page: {page_title}\n"
            f"Type: {type_name}\n"
            f"Required attributes: {req_text}\n"
            f"Valid statuses: {status_text}\n\n"
            f"## Ontology\n{ontology_summary}\n\n"
            f"## Linked Context (pages that reference this entity)\n{linked_context}\n\n"
            "Enrich this stub with attributes derived from the linked context."
        )

        enrich_stub_tool = {
            "name": "enrich_stub_result",
            "description": "Return the enrichment result for a stub page.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["enrich", "delete", "skip"],
                        "description": "enrich: add fields. delete: remove garbage. skip: insufficient context.",
                    },
                    "fields": {
                        "type": "object",
                        "description": "Attribute names and values to add. Only for action 'enrich'.",
                    },
                    "reasoning": {
                        "type": "string",
                        "description": "Brief explanation of the decision.",
                    },
                },
                "required": ["action", "reasoning"],
            },
        }

        response = self._client.messages.create(
            model=self._model,
            max_tokens=1024,
            system=system,
            tools=[enrich_stub_tool],
            tool_choice={"type": "tool", "name": "enrich_stub_result"},
            messages=[{"role": "user", "content": user}],
        )

        for block in response.content:
            if block.type == "tool_use" and block.name == "enrich_stub_result":
                return block.input

        return {"action": "skip", "reasoning": "No enrichment suggested by LLM"}
