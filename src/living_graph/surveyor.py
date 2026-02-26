# ABOUTME: Surveyor pipeline — 4-stage semantic mapping for the knowledge graph.
# ABOUTME: Embeds pages, clusters by similarity, labels via LLM, writes tags + relationships.

from __future__ import annotations

import json
import os
import time
from datetime import date

from living_graph.client import RoamClient
from living_graph.clustering import merge_clusters, semantic_clusters, structural_clusters
from living_graph.embeddings import OllamaEmbedder, extract_page_text
from living_graph.interlinking import Interlinker
from living_graph.llm import LLMClient
from living_graph.mutation_log import MutationLogger
from living_graph.scope import ScopeEnforcer
from living_graph.vector_store import VectorStore


class SurveyorPipeline:
    """4-stage surveyor pipeline: Embed -> Cluster -> Label -> Write.

    Discovers semantic and structural connections between pages.
    Writes relationship tags and Related:: links. Cannot create or delete pages.
    """

    STATE_FILE = "surveyor_state.json"

    def __init__(
        self,
        roam: RoamClient,
        claude=None,
        data_dir: str = "data",
        run_prefix: str = "Run/",
        ollama_model: str = "nomic-embed-text",
        min_cluster_size: int = 3,
    ):
        self._roam = roam
        self._llm = LLMClient(claude) if claude else None
        self._embedder = OllamaEmbedder(model=ollama_model)
        self._interlinker = Interlinker(roam)
        self._logger = MutationLogger(roam, namespace_prefix=run_prefix)
        self._scope = ScopeEnforcer("surveyor")
        self._data_dir = data_dir
        self._min_cluster_size = min_cluster_size

        os.makedirs(data_dir, exist_ok=True)
        db_path = os.path.join(data_dir, "surveyor.db")
        self._store = VectorStore(db_path=db_path)

    def _load_state(self) -> dict:
        """Load persisted state (page hashes, last run timestamp)."""
        path = os.path.join(self._data_dir, self.STATE_FILE)
        if os.path.exists(path):
            with open(path) as f:
                return json.load(f)
        return {"page_hashes": {}, "last_run": None}

    def _save_state(self, state: dict) -> None:
        """Persist state to disk."""
        path = os.path.join(self._data_dir, self.STATE_FILE)
        with open(path, "w") as f:
            json.dump(state, f, indent=2)

    def _get_typed_pages(self) -> list[str]:
        """Query Roam for all pages with namespace prefixes."""
        namespaces = [
            "Person/", "Org/", "Project/", "Tool/", "Process/",
            "Decision/", "Assumption/", "Constraint/", "Contradiction/",
            "Synthesis/", "Location/", "Event/", "Account/", "Asset/",
        ]
        all_pages = []
        for ns in namespaces:
            results = self._roam.q(
                '[:find ?title :where '
                '[?p :node/title ?title] '
                '[(clojure.string/starts-with? ?title "' + ns + '")]]'
            )
            all_pages.extend(row[0] for row in results)
            time.sleep(0.5)
        return sorted(set(all_pages))

    def _build_ref_graph(self, page_titles: list[str]) -> dict[str, set[str]]:
        """Build adjacency list from Roam's block refs.

        For each page, find which other pages in our set it references.
        Rate-limited to avoid hitting Roam API limits.
        """
        title_set = set(page_titles)
        refs: dict[str, set[str]] = {}
        for title in page_titles:
            results = self._roam.q(
                '[:find ?ref-title :where '
                f'[?p :node/title "{title}"] '
                '[?b :block/page ?p] '
                '[?b :block/refs ?ref-page] '
                '[?ref-page :node/title ?ref-title]]'
            )
            page_refs = {
                row[0] for row in results
                if row[0] in title_set and row[0] != title
            }
            if page_refs:
                refs[title] = page_refs
            time.sleep(0.5)
        return refs

    def survey(self, page_titles: list[str] | None = None) -> dict:
        """Run the full 4-stage survey pipeline.

        Args:
            page_titles: Pages to survey. If None, surveys all typed pages.

        Returns:
            Dict with keys: pages_embedded, clusters_found,
            relationships_written, tags_written, run_uid.
        """
        today = date.today().isoformat()
        run = self._logger.create_run("Surveyor", today)

        if page_titles is None:
            page_titles = self._get_typed_pages()

        if not page_titles:
            self._logger.close_run(
                run["uid"], "completed", "0 pages — nothing to survey"
            )
            return {
                "pages_embedded": 0,
                "clusters_found": 0,
                "relationships_written": 0,
                "tags_written": 0,
                "run_uid": run["uid"],
            }

        # --- Stage 1: Embed ---
        state = self._load_state()
        embedded_count = 0

        for title in page_titles:
            text = extract_page_text(self._roam, title)
            if not text:
                continue

            # Check if page content changed (simple hash for incremental)
            text_hash = str(hash(text))
            if state["page_hashes"].get(title) == text_hash:
                continue  # Skip unchanged pages

            vector = self._embedder.embed(text)
            self._store.upsert(title, vector, {"title": title})
            state["page_hashes"][title] = text_hash
            embedded_count += 1
            time.sleep(0.5)

        self._logger.log(
            run["uid"], "embed", "batch",
            {"pages_embedded": embedded_count, "total_pages": len(page_titles)},
        )

        # --- Stage 2: Cluster ---
        all_vecs = self._store.get_all()
        vectors = {v["id"]: v["vector"] for v in all_vecs}

        sem_clusters = semantic_clusters(
            vectors, min_cluster_size=self._min_cluster_size
        )
        ref_graph = self._build_ref_graph(page_titles)
        struct_clusters = structural_clusters(ref_graph)
        merged = merge_clusters(sem_clusters, struct_clusters)

        self._logger.log(
            run["uid"], "cluster", "batch",
            {
                "semantic": len(sem_clusters),
                "structural": len(struct_clusters),
                "merged": len(merged),
            },
        )

        if not merged:
            state["last_run"] = today
            self._save_state(state)
            self._logger.close_run(
                run["uid"], "completed",
                f"{embedded_count} embedded, 0 clusters",
            )
            return {
                "pages_embedded": embedded_count,
                "clusters_found": 0,
                "relationships_written": 0,
                "tags_written": 0,
                "run_uid": run["uid"],
            }

        # --- Stage 3: Label ---
        labeled_clusters = []
        for cluster in merged:
            cluster_pages = []
            for title in cluster:
                text = extract_page_text(self._roam, title)
                cluster_pages.append({"title": title, "text": text[:500]})
                time.sleep(0.5)

            labels = self._llm.label_cluster(cluster_pages)
            labeled_clusters.append({
                "pages": cluster,
                "tags": labels.get("tags", []),
                "relationships": labels.get("relationships", []),
            })

            self._logger.log(
                run["uid"], "label", f"cluster ({len(cluster)} pages)",
                {"tags": labels.get("tags", [])},
            )
            time.sleep(1)

        # --- Stage 4: Write ---
        tags_written = 0
        relationships_written = 0

        for cluster_data in labeled_clusters:
            # Write tags
            for title in cluster_data["pages"]:
                self._scope.check("edit_tags")
                page_results = self._roam.q(
                    f'[:find ?uid :where [?p :node/title "{title}"] [?p :block/uid ?uid]]'
                )
                if not page_results:
                    continue
                page_uid = page_results[0][0]

                for tag in cluster_data["tags"]:
                    tag_text = f"#surveyor/{tag}"
                    self._roam.create_block(page_uid, tag_text, "last")
                    tags_written += 1
                    time.sleep(0.5)

                self._logger.log(
                    run["uid"], "tag", title,
                    {"tags": cluster_data["tags"]},
                )
                time.sleep(0.5)

            # Write relationships
            for rel in cluster_data["relationships"]:
                self._scope.check("edit_relationships")

                source_results = self._roam.q(
                    '[:find ?uid :where '
                    f'[?p :node/title "{rel["source"]}"] '
                    '[?p :block/uid ?uid]]'
                )
                if not source_results:
                    continue
                source_uid = source_results[0][0]

                self._interlinker.add_related(source_uid, [rel["target"]])
                relationships_written += 1

                self._logger.log(
                    run["uid"], "link", rel["source"],
                    {"target": rel["target"], "type": rel["type"]},
                )
                time.sleep(0.5)

        # --- Close run ---
        state["last_run"] = today
        self._save_state(state)

        summary = (
            f"{embedded_count} embedded, {len(merged)} clusters, "
            f"{tags_written} tags, {relationships_written} relationships"
        )
        self._logger.close_run(run["uid"], "completed", summary)

        return {
            "pages_embedded": embedded_count,
            "clusters_found": len(merged),
            "relationships_written": relationships_written,
            "tags_written": tags_written,
            "run_uid": run["uid"],
        }
