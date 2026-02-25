# ABOUTME: Distiller pipeline — 4-stage epistemic knowledge extraction.
# ABOUTME: Surfaces implicit assumptions, decisions, constraints, contradictions, syntheses.

from __future__ import annotations

import time
from datetime import date

from living_graph.client import RoamClient
from living_graph.context import GraphContext
from living_graph.entity_resolution import EntityResolver
from living_graph.llm import LLMClient
from living_graph.mutation_log import MutationLogger
from living_graph.ontology import OntologyParser
from living_graph.scope import ScopeEnforcer, EPISTEMIC_NAMESPACES


class DistillerPipeline:
    """4-stage distiller pipeline: Distill → Resolve → Populate → Enrich.

    Reads daily page blocks and surfaces implicit epistemic knowledge.
    Can only create pages in epistemic namespaces. Cannot edit or delete.
    Dedup via entity resolution — no tracking tags needed.
    """

    PROFILE_PAGE = "Convention/David's Preferences"

    def __init__(
        self,
        roam: RoamClient,
        claude=None,
        run_prefix: str = "Run/",
    ):
        self._roam = roam
        self._llm = LLMClient(claude) if claude else None
        self._resolver = EntityResolver(roam)
        self._logger = MutationLogger(roam, namespace_prefix=run_prefix)
        self._context = GraphContext(roam)
        self._parser = OntologyParser(roam)
        self._scope = ScopeEnforcer("distiller")

    def _get_page_blocks(self, page_title: str) -> list[dict]:
        """Get all top-level blocks from a page.

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
            {"uid": c[":block/uid"], "string": c.get(":block/string", "")}
            for c in children
            if c.get(":block/string", "").strip()
        ]

    def build_epistemic_context(self) -> str:
        """Build a context string of existing epistemic pages only.

        Returns a compact listing of Assumption/, Decision/, Constraint/,
        Contradiction/, and Synthesis/ pages so the LLM knows what
        epistemic knowledge already exists.
        """
        sections = []
        for ns in sorted(EPISTEMIC_NAMESPACES):
            pages = self._roam.q(
                '[:find ?title :where '
                '[?p :node/title ?title] '
                '[(clojure.string/starts-with? ?title "'
                + ns
                + '")]]'
            )
            if not pages:
                sections.append(f"## {ns} (0 pages)")
                continue

            titles = sorted(row[0] for row in pages)
            header = f"## {ns} ({len(titles)} page{'s' if len(titles) != 1 else ''})"
            listing = "\n".join(f"- {t}" for t in titles)
            sections.append(f"{header}\n{listing}")

        return "\n\n".join(sections)

    def build_epistemic_ontology_summary(self) -> str:
        """Build an ontology summary limited to epistemic types."""
        types = self._parser.parse()
        epistemic_names = {
            ns.rstrip("/") for ns in EPISTEMIC_NAMESPACES
        }
        lines = []
        for td in sorted(types.values(), key=lambda t: t.namespace):
            if td.name not in epistemic_names:
                continue
            req = ", ".join(td.required) if td.required else "none"
            statuses = ", ".join(td.statuses) if td.statuses else "none"
            lines.append(
                f"- {td.name} (namespace: {td.namespace}, "
                f"required: {req}, statuses: {statuses})"
            )
        return "Epistemic types:\n" + "\n".join(lines)

    def load_user_profile(self) -> str:
        """Load the graph owner's profile for relevance filtering."""
        results = self._roam.q(
            '[:find ?uid :where '
            f'[?p :node/title "{self.PROFILE_PAGE}"] '
            '[?p :block/uid ?uid]]'
        )
        if not results:
            return ""

        page_uid = results[0][0]
        tree = self._roam.pull(
            "[:block/uid {:block/children [:block/string :block/order {:block/children ...}]}]",
            f'[:block/uid "{page_uid}"]',
        )
        children = sorted(
            tree.get(":block/children", []),
            key=lambda b: b.get(":block/order", 0),
        )

        lines = []
        for child in children:
            text = child.get(":block/string", "")
            if text:
                lines.append(f"- {text}")
            for grandchild in sorted(
                child.get(":block/children", []),
                key=lambda b: b.get(":block/order", 0),
            ):
                gc_text = grandchild.get(":block/string", "")
                if gc_text:
                    lines.append(f"  - {gc_text}")

        return "\n".join(lines)

    def distill_page(self, page_title: str) -> dict:
        """Run the full 4-stage pipeline on a page.

        Returns dict with keys: blocks_processed, insights_extracted,
        pages_created, pages_resolved, run_uid.
        """
        today = date.today().isoformat()
        run = self._logger.create_run("Distiller", today)

        # Get all blocks from the page
        blocks = self._get_page_blocks(page_title)
        if not blocks:
            self._logger.close_run(
                run["uid"], "completed", "0 blocks — nothing to distill"
            )
            return {
                "blocks_processed": 0,
                "insights_extracted": 0,
                "pages_created": 0,
                "pages_resolved": 0,
                "run_uid": run["uid"],
            }

        block_texts = [b["string"] for b in blocks]

        # --- Stage 1: Distill (LLM) ---
        epistemic_context = self.build_epistemic_context()
        ontology_summary = self.build_epistemic_ontology_summary()
        user_profile = self.load_user_profile()

        manifest = self._llm.distill_insights(
            blocks=block_texts,
            graph_context=epistemic_context,
            ontology_summary=ontology_summary,
            user_profile=user_profile,
        )

        self._logger.log(
            run["uid"],
            "distill",
            page_title,
            {"blocks": len(block_texts), "insights_found": len(manifest.entities)},
        )

        if not manifest.entities:
            self._logger.close_run(
                run["uid"], "completed",
                f"{len(block_texts)} blocks, 0 insights",
            )
            return {
                "blocks_processed": len(block_texts),
                "insights_extracted": 0,
                "pages_created": 0,
                "pages_resolved": 0,
                "run_uid": run["uid"],
            }

        # --- Stage 2: Resolve (deterministic) ---
        resolved = []
        created_count = 0
        resolved_count = 0

        for entity in manifest.entities:
            namespace = entity["type"].capitalize() + "/"
            self._scope.check("create", namespace)

            result = self._resolver.resolve_or_create(namespace, entity["name"])
            resolved.append({**entity, **result})

            if result["created"]:
                created_count += 1
                self._logger.log(
                    run["uid"], "create", result["title"],
                    {"type": entity["type"]},
                )
            else:
                resolved_count += 1
                self._logger.log(
                    run["uid"], "resolve_existing", result["title"],
                    {"type": entity["type"]},
                )

            time.sleep(1)

        # --- Stage 3: Populate (deterministic) ---
        for entity in resolved:
            if not entity.get("created"):
                continue  # Only populate new pages

            page_uid = entity["uid"]
            fields = entity.get("fields", {})

            # Always set Source:: pointing to the daily page
            if "Source" not in fields:
                fields["Source"] = f"[[{page_title}]]"

            for attr, value in fields.items():
                self._roam.create_block(page_uid, f"{attr}:: {value}", "last")

            # Add the description as a content block
            if entity.get("description"):
                self._roam.create_block(page_uid, entity["description"], "last")

            self._logger.log(
                run["uid"], "populate", entity["title"],
                {"attrs_set": list(fields.keys())},
            )
            time.sleep(1)

        # --- Stage 4: Enrich (LLM) ---
        enriched_count = 0
        for entity in resolved:
            if not entity.get("created"):
                continue  # Only enrich new pages

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
                for attr, value in enrichment.items():
                    if attr not in current_attrs or not current_attrs[attr]:
                        self._roam.create_block(
                            entity["uid"], f"{attr}:: {value}", "last"
                        )
                enriched_count += 1
                self._logger.log(
                    run["uid"], "enrich", entity["title"],
                    {"fields_added": list(enrichment.keys())},
                )

        # --- Close run ---
        summary = (
            f"{len(block_texts)} blocks, {len(manifest.entities)} insights, "
            f"{created_count} created, {resolved_count} existing, "
            f"{enriched_count} enriched"
        )
        self._logger.close_run(run["uid"], "completed", summary)

        return {
            "blocks_processed": len(block_texts),
            "insights_extracted": len(manifest.entities),
            "pages_created": created_count,
            "pages_resolved": resolved_count,
            "run_uid": run["uid"],
        }
