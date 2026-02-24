# ABOUTME: Janitor pipeline — 3-stage graph maintenance (autofix, link repair, enrich).
# ABOUTME: Deterministic fixes first, LLM only for ambiguous cases. Cannot create pages.

from __future__ import annotations

import re
import time
from datetime import date

from living_graph.client import RoamClient
from living_graph.llm import LLMClient
from living_graph.mutation_log import MutationLogger
from living_graph.ontology import OntologyParser
from living_graph.scope import ScopeEnforcer
from living_graph.validation import (
    NAMESPACE_CORRECTIONS,
    Issue,
    ValidationScanner,
)


class JanitorPipeline:
    """3-stage janitor pipeline: Autofix → Link Repair → Enrich.

    Stage 1 (Autofix): Deterministic Python fixes — no LLM calls.
        Fixes invalid statuses, namespace typos, broken link bare text.
        Flags duplicates, orphans, floating tasks with Janitor Note.

    Stage 2 (Link Repair): Hybrid — Python-first for unambiguous matches,
        LLM for ambiguous broken link resolution.

    Stage 3 (Enrich): LLM per-file — fills stubs using linked page context.
        Never invents information. Skips if insufficient context.

    Scope: read, edit, delete. Cannot create pages.
    Modes: light (Stage 1 only), deep (all 3 stages).
    """

    PULL_CHILDREN = "[:block/uid :block/string :block/order {:block/children ...}]"

    def __init__(
        self,
        roam: RoamClient,
        claude=None,
        run_prefix: str = "Run/",
    ):
        self._roam = roam
        self._llm = LLMClient(claude) if claude else None
        self._scanner = ValidationScanner(roam)
        self._logger = MutationLogger(roam, namespace_prefix=run_prefix)
        self._parser = OntologyParser(roam)
        self._scope = ScopeEnforcer("janitor")

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

    def scan(
        self,
        namespaces: list[str] | None = None,
        type_name: str | None = None,
    ) -> dict[str, list[Issue]]:
        """Run the validation scanner.

        Args:
            namespaces: Limit to specific namespace prefixes. If None, scan all.
            type_name: Explicit type name for validation (used with custom namespaces).

        Returns:
            Dict mapping page title -> list of issues.
        """
        if namespaces:
            report: dict[str, list[Issue]] = {}
            for ns in namespaces:
                report.update(
                    self._scanner.scan_namespace(ns, type_name=type_name)
                )
            return report
        return self._scanner.scan_all()

    # --- Stage 1: Deterministic Autofix ---

    def _autofix_issue(self, issue: Issue, run_uid: str) -> str:
        """Attempt a deterministic fix for an issue. No LLM calls.

        Returns: "fixed", "flagged", or "skipped".
        """
        kind = issue.kind

        if kind == "invalid_status":
            return self._fix_invalid_status(issue, run_uid)
        if kind == "broken_link":
            return self._fix_broken_link_bare_text(issue, run_uid)
        if kind == "wrong_namespace":
            # Flag — namespace rename is destructive and needs human review
            return self._flag_issue(issue, run_uid)
        if kind in ("duplicate", "orphan", "floating_task"):
            # Flag for human review
            return self._flag_issue(issue, run_uid)
        if kind == "missing_attr":
            # Attempt deterministic inference for specific attrs
            return self._fix_missing_attr(issue, run_uid)
        if kind == "stub":
            # Handled in Stage 3 (enrich)
            return "skipped"

        return "skipped"

    def _fix_invalid_status(self, issue: Issue, run_uid: str) -> str:
        """Fix an invalid status using the correction mapping."""
        correction = issue.meta.get("suggested_correction")
        block_uid = issue.meta.get("block_uid")

        if not correction or not block_uid:
            return self._flag_issue(issue, run_uid)

        self._scope.check("edit", issue.page_title)
        self._roam.update_block(block_uid, f"Status:: {correction}")
        self._logger.log(
            run_uid, "fix_status", issue.page_title,
            {"old": issue.meta.get("current_value"), "new": correction},
        )
        return "fixed"

    def _fix_broken_link_bare_text(self, issue: Issue, run_uid: str) -> str:
        """Remove bare text from Related:: block (deleted page remnants)."""
        block_uid = issue.meta.get("block_uid")
        current_value = issue.meta.get("current_value", "")

        if not block_uid:
            return "skipped"

        # Keep only valid [[...]] links, strip bare text
        valid_links = re.findall(r"\[\[[^\]]+\]\]", current_value)
        if valid_links:
            new_value = "Related:: " + " ".join(valid_links)
        else:
            # All links were broken — clear the value
            new_value = "Related::"

        self._scope.check("edit", issue.page_title)
        self._roam.update_block(block_uid, new_value)
        self._logger.log(
            run_uid, "fix_broken_link", issue.page_title,
            {"bare_text_removed": issue.meta.get("bare_text", "")},
        )
        return "fixed"

    def _fix_missing_attr(self, issue: Issue, run_uid: str) -> str:
        """Attempt deterministic inference for missing attributes.

        Only infers attrs we can confidently derive without LLM:
        - Status:: → first valid status for the type (if type has statuses)
        """
        attr_name = issue.detail
        typedef = self._scanner._resolve_type(issue.page_title)

        # We can infer Status with a safe default if the type defines statuses
        if attr_name == "Status" and typedef and typedef.statuses:
            # Default to first listed status (typically "active" or "todo")
            default_status = typedef.statuses[0]
            page_uid = self._get_page_uid(issue.page_title)
            if page_uid:
                self._scope.check("edit", issue.page_title)
                self._roam.create_block(
                    page_uid, f"Status:: {default_status}", "last"
                )
                self._logger.log(
                    run_uid, "infer_attr", issue.page_title,
                    {"attr": "Status", "value": default_status, "method": "default"},
                )
                return "fixed"

        # Other missing attrs need LLM or human — flag
        return self._flag_issue(issue, run_uid)

    def _flag_issue(self, issue: Issue, run_uid: str) -> str:
        """Add a Janitor Note:: to the page flagging the issue for review."""
        page_uid = self._get_page_uid(issue.page_title)
        if not page_uid:
            return "skipped"

        # Check if a Janitor Note already exists for this issue
        tree = self._roam.pull(
            "[:block/uid {:block/children [:block/uid :block/string]}]",
            f'[:block/uid "{page_uid}"]',
        )
        children = tree.get(":block/children", [])
        existing_notes = [
            c for c in children
            if c.get(":block/string", "").startswith("Janitor Note::")
        ]

        # Don't pile up duplicate notes
        note_text = f"Janitor Note:: {issue.kind} — {issue.detail}"
        for note in existing_notes:
            if issue.kind in note.get(":block/string", ""):
                return "skipped"

        self._scope.check("edit", issue.page_title)
        self._roam.create_block(page_uid, note_text, "last")
        self._logger.log(
            run_uid, "flag", issue.page_title,
            {"issue": issue.kind, "detail": issue.detail},
        )
        return "flagged"

    # --- Stage 2: Link Repair (Hybrid) ---

    def _repair_link(self, issue: Issue, run_uid: str) -> str:
        """Attempt to repair a broken link — Python-first, LLM for ambiguous.

        In Roam, broken links manifest as bare text (brackets stripped after
        page deletion). We search for pages matching the bare text and try
        to re-link.
        """
        bare_text = issue.meta.get("bare_text", "").strip()
        block_uid = issue.meta.get("block_uid")
        current_value = issue.meta.get("current_value", "")

        if not bare_text or not block_uid:
            return "skipped"

        # Search for candidate pages matching the bare text
        candidates = self._find_link_candidates(bare_text)

        if len(candidates) == 1:
            # Unambiguous — fix directly in Python
            target = candidates[0]
            new_link = f"[[{target}]]"
            new_value = current_value.replace(bare_text, new_link)
            new_value = f"Related:: {new_value}" if not new_value.startswith("Related::") else new_value

            self._scope.check("edit", issue.page_title)
            self._roam.update_block(block_uid, new_value)
            self._logger.log(
                run_uid, "repair_link", issue.page_title,
                {"bare_text": bare_text, "resolved_to": target, "method": "unambiguous"},
            )
            return "fixed"

        if len(candidates) > 1 and self._llm:
            # Ambiguous — ask LLM to pick
            fix = self._llm.repair_link(
                page_title=issue.page_title,
                bare_text=bare_text,
                candidates=candidates,
                block_uid=block_uid,
                current_value=current_value,
            )
            if fix["action"] == "edit_block" and fix.get("target_uid") and fix.get("new_value"):
                self._scope.check("edit", issue.page_title)
                self._roam.update_block(fix["target_uid"], fix["new_value"])
                self._logger.log(
                    run_uid, "repair_link", issue.page_title,
                    {"bare_text": bare_text, "method": "llm", "reasoning": fix.get("reasoning", "")},
                )
                return "fixed"
            # LLM couldn't resolve — flag it
            self._flag_issue(issue, run_uid)
            return "flagged"

        # No candidates found — bare text stays removed (Stage 1 already cleaned it)
        return "skipped"

    def _find_link_candidates(self, bare_text: str) -> list[str]:
        """Search for pages that might match bare text from a broken link."""
        # Try exact title match first
        escaped = bare_text.replace('"', '\\"')
        exact = self._roam.q(
            '[:find ?title :where '
            '[?p :node/title ?title] '
            '[(= ?title "' + escaped + '")]]'
        )
        if exact:
            return [exact[0][0]]

        # Try case-insensitive search across namespaced pages
        results = self._roam.q(
            '[:find ?title :where '
            '[?p :node/title ?title] '
            '[(clojure.string/includes? ?title "' + escaped + '")]]'
        )
        candidates = [title for (title,) in results if "/" in title]
        return candidates[:15]  # Cap at 15 like Alfred

    # --- Stage 3: Stub Enrichment (LLM) ---

    def _enrich_stub(self, issue: Issue, run_uid: str) -> str:
        """Enrich a stub page using linked context. LLM fills in attrs."""
        if not self._llm:
            return "skipped"

        page_uid = self._get_page_uid(issue.page_title)
        if not page_uid:
            return "skipped"

        typedef = self._scanner._resolve_type(issue.page_title)
        if not typedef:
            return "skipped"

        # Gather linked context: pages that reference this stub
        linked_context = self._build_linked_context(issue.page_title)
        if not linked_context:
            # No context available — flag rather than guess
            self._flag_issue(issue, run_uid)
            return "flagged"

        ontology_summary = self.build_ontology_summary()

        enrichment = self._llm.enrich_stub(
            page_title=issue.page_title,
            type_name=typedef.name,
            required_attrs=typedef.required,
            valid_statuses=typedef.statuses,
            linked_context=linked_context,
            ontology_summary=ontology_summary,
        )

        if not enrichment or enrichment.get("action") == "skip":
            self._logger.log(
                run_uid, "skip_enrich", issue.page_title,
                {"reason": enrichment.get("reasoning", "insufficient context")},
            )
            return "skipped"

        if enrichment.get("action") == "delete":
            self._scope.check("delete", issue.page_title)
            self._roam.delete_page(page_uid)
            self._logger.log(
                run_uid, "delete_stub", issue.page_title,
                {"reasoning": enrichment.get("reasoning", "")},
            )
            return "fixed"

        # Apply enrichment fields
        fields = enrichment.get("fields", {})
        if fields:
            self._scope.check("edit", issue.page_title)
            for attr, value in fields.items():
                self._roam.create_block(page_uid, f"{attr}:: {value}", "last")
            self._logger.log(
                run_uid, "enrich_stub", issue.page_title,
                {"fields_added": list(fields.keys()), "reasoning": enrichment.get("reasoning", "")},
            )
            return "fixed"

        return "skipped"

    def _build_linked_context(self, page_title: str) -> str:
        """Gather context from pages that reference this page.

        Returns formatted string of linked page content, truncated per page.
        """
        escaped = page_title.replace('"', '\\"')
        results = self._roam.q(
            '[:find ?source-title :where '
            '[?target :node/title "' + escaped + '"] '
            '[?b :block/refs ?target] '
            '[?b :block/page ?source] '
            '[?source :node/title ?source-title] '
            '[(not= ?source-title "' + escaped + '")]]'
        )

        if not results:
            return ""

        context_parts = []
        for (source_title,) in results[:10]:  # Cap at 10 linked pages
            _, blocks, attrs = self._scanner._get_page_data(source_title)
            if not blocks:
                continue
            lines = [f"### [[{source_title}]]"]
            for b in blocks[:20]:  # Cap at 20 blocks per page
                text = b["string"]
                if len(text) > 200:
                    text = text[:200] + "..."
                lines.append(f"  - {text}")
            context_parts.append("\n".join(lines))

        return "\n\n".join(context_parts)

    # --- Pipeline orchestration ---

    def _get_page_uid(self, title: str) -> str | None:
        """Get the UID for a page by title."""
        escaped = title.replace('"', '\\"')
        results = self._roam.q(
            '[:find ?uid :where [?p :node/title "' + escaped + '"] [?p :block/uid ?uid]]'
        )
        return results[0][0] if results else None

    def run(
        self,
        namespaces: list[str] | None = None,
        type_name: str | None = None,
        deep: bool = True,
    ) -> dict:
        """Run the janitor pipeline.

        Args:
            namespaces: Limit to specific namespace prefixes. If None, scan all.
            type_name: Explicit type name for validation (used with custom namespaces).
            deep: If True, run all 3 stages. If False, Stage 1 only (light sweep).

        Returns:
            Dict with: issues_found, fixed, flagged, skipped, pages_scanned,
            run_uid, mode.
        """
        today = date.today().isoformat()
        mode = "deep" if deep else "light"
        run = self._logger.create_run("Janitor", today)

        # --- Scan ---
        report = self.scan(namespaces=namespaces, type_name=type_name)
        total_issues = sum(len(issues) for issues in report.values())

        self._logger.log(
            run["uid"],
            "scan",
            "all namespaces" if not namespaces else ", ".join(namespaces),
            {"pages_scanned": len(report), "issues_found": total_issues, "mode": mode},
        )

        if not total_issues:
            self._logger.close_run(
                run["uid"], "completed",
                f"{len(report)} pages scanned, 0 issues — graph is clean",
            )
            return {
                "issues_found": 0, "fixed": 0, "flagged": 0, "skipped": 0,
                "pages_scanned": len(report), "run_uid": run["uid"], "mode": mode,
            }

        fixed = 0
        flagged = 0
        skipped = 0
        stub_issues: list[Issue] = []
        broken_link_issues: list[Issue] = []

        # --- Stage 1: Deterministic Autofix ---
        for page_title, issues in report.items():
            for issue in issues:
                if issue.kind == "stub":
                    stub_issues.append(issue)
                    continue
                if issue.kind == "broken_link":
                    broken_link_issues.append(issue)
                    # Still do bare text cleanup in Stage 1
                    pass

                result = self._autofix_issue(issue, run["uid"])
                if result == "fixed":
                    fixed += 1
                elif result == "flagged":
                    flagged += 1
                else:
                    skipped += 1
                time.sleep(0.5)

        self._logger.log(
            run["uid"], "stage1_complete", "autofix",
            {"fixed": fixed, "flagged": flagged, "skipped": skipped},
        )

        if not deep:
            summary = (
                f"Light sweep: {len(report)} pages, {total_issues} issues, "
                f"{fixed} fixed, {flagged} flagged, {skipped} skipped"
            )
            self._logger.close_run(run["uid"], "completed", summary)
            return {
                "issues_found": total_issues, "fixed": fixed, "flagged": flagged,
                "skipped": skipped, "pages_scanned": len(report),
                "run_uid": run["uid"], "mode": mode,
            }

        # --- Stage 2: Link Repair (hybrid) ---
        s2_fixed = 0
        s2_flagged = 0
        for issue in broken_link_issues:
            result = self._repair_link(issue, run["uid"])
            if result == "fixed":
                s2_fixed += 1
            elif result == "flagged":
                s2_flagged += 1
            time.sleep(1)

        fixed += s2_fixed
        flagged += s2_flagged

        self._logger.log(
            run["uid"], "stage2_complete", "link_repair",
            {"fixed": s2_fixed, "flagged": s2_flagged},
        )

        # --- Stage 3: Stub Enrichment (LLM) ---
        s3_fixed = 0
        s3_flagged = 0
        s3_skipped = 0
        for issue in stub_issues:
            result = self._enrich_stub(issue, run["uid"])
            if result == "fixed":
                s3_fixed += 1
            elif result == "flagged":
                s3_flagged += 1
            else:
                s3_skipped += 1
            time.sleep(1)

        fixed += s3_fixed
        flagged += s3_flagged
        skipped += s3_skipped

        self._logger.log(
            run["uid"], "stage3_complete", "enrich",
            {"fixed": s3_fixed, "flagged": s3_flagged, "skipped": s3_skipped},
        )

        # --- Close run ---
        summary = (
            f"Deep sweep: {len(report)} pages, {total_issues} issues, "
            f"{fixed} fixed, {flagged} flagged, {skipped} skipped"
        )
        self._logger.close_run(run["uid"], "completed", summary)

        return {
            "issues_found": total_issues, "fixed": fixed, "flagged": flagged,
            "skipped": skipped, "pages_scanned": len(report),
            "run_uid": run["uid"], "mode": mode,
        }
