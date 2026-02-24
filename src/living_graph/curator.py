# ABOUTME: Curator pipeline — 4-stage entity extraction and enrichment.
# ABOUTME: LLM stages for analysis/enrichment, deterministic stages for resolution/interlinking.

from __future__ import annotations

import time
from datetime import date

from living_graph.client import RoamClient
from living_graph.context import GraphContext
from living_graph.entity_resolution import EntityResolver
from living_graph.interlinking import Interlinker
from living_graph.llm import LLMClient
from living_graph.mutation_log import MutationLogger
from living_graph.ontology import OntologyParser
from living_graph.scope import ScopeEnforcer


class CuratorPipeline:
    """4-stage curator pipeline: Analyze → Resolve → Interlink → Enrich."""

    def __init__(
        self,
        roam: RoamClient,
        claude=None,
        run_prefix: str = "Run/",
    ):
        self._roam = roam
        self._llm = LLMClient(claude) if claude else None
        self._resolver = EntityResolver(roam)
        self._linker = Interlinker(roam)
        self._logger = MutationLogger(roam, namespace_prefix=run_prefix)
        self._context = GraphContext(roam)
        self._parser = OntologyParser(roam)
        self._scope = ScopeEnforcer("curator")

    def find_uncurated_blocks(self, page_title: str) -> list[dict]:
        """Find blocks on a page that don't have #curated tag.

        Returns list of {"uid": str, "string": str}.
        """
        results = self._roam.q(
            f'[:find ?uid :where [?p :node/title "{page_title}"] [?p :block/uid ?uid]]'
        )
        if not results:
            return []
        page_uid = results[0][0]

        tree = self._roam.pull(
            "[:block/uid {:block/children [:block/uid :block/string :block/order]}]",
            f'[:block/uid "{page_uid}"]',
        )
        children = sorted(
            tree.get(":block/children", []),
            key=lambda b: b.get(":block/order", 0),
        )
        return [
            {"uid": c[":block/uid"], "string": c[":block/string"]}
            for c in children
            if "#curated" not in c.get(":block/string", "")
        ]

    def mark_curated(self, block_uids: list[str]) -> None:
        """Append #curated tag to blocks."""
        for uid in block_uids:
            block = self._roam.pull("[:block/string]", f'[:block/uid "{uid}"]')
            current = block.get(":block/string", "")
            if "#curated" not in current:
                self._roam.update_block(uid, f"{current} #curated")

    def build_ontology_summary(self) -> str:
        """Build a compact ontology summary for LLM context."""
        types = self._parser.parse()
        lines = []
        for td in sorted(types.values(), key=lambda t: t.namespace):
            req = ", ".join(td.required) if td.required else "none"
            statuses = ", ".join(td.statuses) if td.statuses else "none"
            lines.append(
                f"- {td.name} (namespace: {td.namespace}, "
                f"required: {req}, statuses: {statuses})"
            )
        return "Available types:\n" + "\n".join(lines)

    def curate_page(self, page_title: str) -> dict:
        """Run the full 4-stage pipeline on a page.

        Returns dict with keys: blocks_processed, entities_resolved,
        entities_enriched, run_uid.
        """
        today = date.today().isoformat()
        run = self._logger.create_run("Curator", today)

        # Find uncurated blocks
        blocks = self.find_uncurated_blocks(page_title)
        if not blocks:
            self._logger.close_run(
                run["uid"], "completed", "0 blocks — nothing to curate"
            )
            return {
                "blocks_processed": 0,
                "entities_resolved": 0,
                "entities_enriched": 0,
                "run_uid": run["uid"],
            }

        block_texts = [b["string"] for b in blocks]
        block_uids = [b["uid"] for b in blocks]

        # --- Stage 1: Analyze (LLM) ---
        graph_context = self._context.build()
        ontology_summary = self.build_ontology_summary()

        manifest = self._llm.extract_entities(
            blocks=block_texts,
            graph_context=graph_context,
            ontology_summary=ontology_summary,
        )

        self._logger.log(
            run["uid"],
            "analyze",
            page_title,
            {"blocks": len(block_texts), "entities_found": len(manifest.entities)},
        )

        # --- Stage 2: Entity Resolution (deterministic) ---
        resolved = []
        for entity in manifest.entities:
            namespace = entity["type"].capitalize() + "/"
            self._scope.check("create", namespace)

            result = self._resolver.resolve_or_create(namespace, entity["name"])
            resolved.append({**entity, **result})

            action = "create" if result["created"] else "resolve"
            self._logger.log(
                run["uid"], action, result["title"], {"created": result["created"]}
            )

            # Set initial attributes on new entities
            if result["created"] and entity.get("fields"):
                time.sleep(1)
                for attr, value in entity["fields"].items():
                    self._roam.create_block(
                        result["uid"], f"{attr}:: {value}", "last"
                    )
                self._logger.log(
                    run["uid"],
                    "edit",
                    result["title"],
                    {"attrs_set": list(entity["fields"].keys())},
                )

        # --- Stage 3: Interlink (deterministic) ---
        page_results = self._roam.q(
            f'[:find ?uid :where [?p :node/title "{page_title}"] [?p :block/uid ?uid]]'
        )
        if page_results:
            page_uid = page_results[0][0]
            for entity in resolved:
                self._linker.link_bidirectional(
                    entity["uid"], entity["title"],
                    page_uid, page_title,
                )
                self._logger.log(
                    run["uid"],
                    "interlink",
                    entity["title"],
                    {"linked_to": page_title},
                )

        # --- Stage 4: Enrich (LLM) ---
        enriched_count = 0
        skip_types = {"location", "event"}
        for entity in resolved:
            if entity["type"] in skip_types:
                continue
            if not entity.get("created"):
                continue  # Only enrich new entities

            # Get current attrs
            time.sleep(1)
            tree = self._roam.pull(
                "[:block/uid {:block/children [:block/string]}]",
                f'[:block/uid "{entity["uid"]}"]',
            )
            children = tree.get(":block/children", [])
            current_attrs = {}
            for child in children:
                text = child.get(":block/string", "")
                if "::" in text:
                    k, v = text.split("::", 1)
                    current_attrs[k.strip()] = v.strip()

            enrichment = self._llm.enrich_entity(
                entity_type=entity["type"],
                entity_name=entity["name"],
                current_attrs=current_attrs,
                source_blocks=block_texts,
                ontology_summary=ontology_summary,
            )

            if enrichment:
                self._scope.check("edit", entity["type"].capitalize() + "/")
                for attr, value in enrichment.items():
                    if attr not in current_attrs or not current_attrs[attr]:
                        self._roam.create_block(
                            entity["uid"], f"{attr}:: {value}", "last"
                        )
                enriched_count += 1
                self._logger.log(
                    run["uid"],
                    "enrich",
                    entity["title"],
                    {"fields_added": list(enrichment.keys())},
                )

        # Mark blocks as curated
        self.mark_curated(block_uids)

        # Close the run
        summary = (
            f"{len(blocks)} blocks, {len(resolved)} entities, "
            f"{enriched_count} enriched"
        )
        self._logger.close_run(run["uid"], "completed", summary)

        return {
            "blocks_processed": len(blocks),
            "entities_resolved": len(resolved),
            "entities_enriched": enriched_count,
            "run_uid": run["uid"],
        }
