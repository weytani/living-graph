# ABOUTME: Validation scanner that checks Roam pages against ontology type definitions.
# ABOUTME: Detects 10 issue types across structural, link, and semantic categories.

from __future__ import annotations

import re
from dataclasses import dataclass, field

from living_graph.entity_resolution import EntityResolver
from living_graph.ontology import OntologyParser, TypeDef


@dataclass
class Issue:
    """A single validation issue found on a page."""

    kind: str
    severity: str  # critical, warning, info
    page_title: str
    detail: str
    # Optional structured data for the fix stages
    meta: dict = field(default_factory=dict)


# Alfred issue code mapping (Alfred code → our kind)
# FM001 → missing_attr
# FM002 → wrong_namespace
# FM003 → invalid_status
# FM004 → malformed_attr (list fields not proper wikilinks)
# LINK001 → broken_link
# ORPHAN001 → orphan
# STUB001 → stub
# DUP001 → duplicate
# SEM004 → floating_task (Task/ without Project:: link)
# (SEM001/SEM003/SEM005/SEM006 detected in Stage 3 by LLM, not here)

_SEVERITY = {
    "missing_attr": "warning",
    "invalid_status": "critical",
    "wrong_namespace": "critical",
    "malformed_attr": "warning",
    "broken_link": "critical",
    "orphan": "warning",
    "stub": "info",
    "duplicate": "warning",
    "floating_task": "warning",
}

# Common namespace typos/plurals → canonical namespace
NAMESPACE_CORRECTIONS = {
    "Persons/": "Person/",
    "People/": "Person/",
    "Organizations/": "Org/",
    "Organisation/": "Org/",
    "Organisations/": "Org/",
    "Organization/": "Org/",
    "Orgs/": "Org/",
    "Projects/": "Project/",
    "Tasks/": "Task/",
    "Sessions/": "Session/",
    "Threads/": "Thread/",
    "Conversations/": "Conversation/",
    "Events/": "Event/",
    "Processes/": "Process/",
    "Runs/": "Run/",
    "Notes/": "Note/",
    "Locations/": "Location/",
    "Accounts/": "Account/",
    "Assets/": "Asset/",
    "Assumptions/": "Assumption/",
    "Constraints/": "Constraint/",
    "Contradictions/": "Contradiction/",
    "Decisions/": "Decision/",
    "Syntheses/": "Synthesis/",
}

# Status correction mapping: common typos/aliases → canonical status
# Type-specific where needed (key = (type_name, bad_status) or (None, bad_status))
STATUS_CORRECTIONS: dict[tuple[str | None, str], str] = {
    # Universal
    (None, "Active"): "active",
    (None, "Inactive"): "inactive",
    (None, "Done"): "done",
    (None, "Completed"): "completed",
    (None, "Archived"): "archived",
    (None, "Cancelled"): "cancelled",
    (None, "Canceled"): "cancelled",
    # Task-specific
    ("Task", "closed"): "done",
    ("Task", "open"): "active",
    ("Task", "in progress"): "active",
    ("Task", "in-progress"): "active",
    ("Task", "wip"): "active",
    ("Task", "pending"): "todo",
    # Project-specific
    ("Project", "done"): "completed",
    ("Project", "on hold"): "paused",
    ("Project", "on-hold"): "paused",
    ("Project", "stopped"): "paused",
    ("Project", "finished"): "completed",
    # Assumption-specific
    ("Assumption", "confirmed"): "validated",
    ("Assumption", "disproven"): "invalidated",
    ("Assumption", "false"): "invalidated",
    ("Assumption", "true"): "validated",
    # Thread-specific
    ("Thread", "done"): "resolved",
    ("Thread", "closed"): "resolved",
    ("Thread", "open"): "active",
}


class ValidationScanner:
    """Scans Roam pages for ontology compliance issues.

    Detects structural issues (missing attrs, invalid statuses, wrong namespaces),
    link issues (broken links, orphans), content issues (stubs, floating tasks),
    and duplicates.
    """

    PULL_SELECTOR = "[:block/uid :block/string :block/order {:block/children ...}]"

    def __init__(self, client):
        self._client = client
        self._parser = OntologyParser(client)
        self._resolver = EntityResolver(client)
        self._types: dict[str, TypeDef] | None = None

    def _get_types(self) -> dict[str, TypeDef]:
        """Lazily parse and cache the ontology."""
        if self._types is None:
            self._types = self._parser.parse()
        return self._types

    def _resolve_type(self, title: str, type_name: str | None = None) -> TypeDef | None:
        """Resolve the TypeDef for a page, either by explicit name or by namespace match."""
        types = self._get_types()
        if type_name:
            return types.get(type_name)
        # Auto-detect from title prefix
        for td in types.values():
            if td.namespace and title.startswith(td.namespace):
                return td
        return None

    def _has_incoming_refs(self, title: str) -> bool:
        """Check if any blocks on other pages reference this page."""
        escaped = title.replace('"', '\\"')
        results = self._client.q(
            '[:find ?source-title :where '
            '[?target :node/title "' + escaped + '"] '
            '[?b :block/refs ?target] '
            '[?b :block/page ?source] '
            '[?source :node/title ?source-title] '
            '[(not= ?source-title "' + escaped + '")]]'
        )
        return len(results) > 0

    def _get_page_data(self, title: str) -> tuple[str | None, list[dict], dict[str, str]]:
        """Fetch a page's UID, children blocks, and parsed attributes.

        Returns (page_uid, children_list, attrs_dict) where children_list
        contains {"uid": str, "string": str} dicts and attrs_dict maps
        attribute name -> value.
        """
        results = self._client.q(
            '[:find ?uid :where '
            '[?p :node/title ?title] '
            '[(= ?title "' + title.replace('"', '\\"') + '")] '
            '[?p :block/uid ?uid]]'
        )
        if not results:
            return None, [], {}

        page_uid = results[0][0]
        tree = self._client.pull(self.PULL_SELECTOR, f'[:block/uid "{page_uid}"]')

        children = sorted(
            tree.get(":block/children", []),
            key=lambda b: b.get(":block/order", 0),
        )

        blocks = [
            {"uid": c.get(":block/uid", ""), "string": c.get(":block/string", "")}
            for c in children
        ]
        attrs: dict[str, str] = {}
        for b in blocks:
            s = b["string"]
            if "::" in s:
                key, _, value = s.partition("::")
                attrs[key.strip()] = value.strip()

        return page_uid, blocks, attrs

    # Keep backward compat for existing tests
    def _get_page_attrs(self, title: str) -> tuple[list[str], dict[str, str]]:
        """Fetch a page's children and parse attribute blocks."""
        _, blocks, attrs = self._get_page_data(title)
        raw_strings = [b["string"] for b in blocks]
        return raw_strings, attrs

    def validate_page(
        self, title: str, type_name: str | None = None
    ) -> list[Issue]:
        """Validate a single page against its type definition.

        Args:
            title: The page title to validate.
            type_name: Explicit type name (e.g. "Person"). If None, auto-detect.

        Returns:
            List of Issue objects found.
        """
        typedef = self._resolve_type(title, type_name)
        if typedef is None:
            return []

        page_uid, blocks, attrs = self._get_page_data(title)
        raw_strings = [b["string"] for b in blocks]
        issues: list[Issue] = []

        # --- Structural checks ---

        # Stub detection: page exists but has no children at all
        if not raw_strings:
            issues.append(Issue(
                kind="stub",
                severity=_SEVERITY["stub"],
                page_title=title,
                detail="Page has no content",
            ))

        # Missing required attributes (FM001)
        for attr_name in typedef.required:
            if attr_name not in attrs:
                issues.append(Issue(
                    kind="missing_attr",
                    severity=_SEVERITY["missing_attr"],
                    page_title=title,
                    detail=attr_name,
                ))

        # Invalid status (FM003)
        if typedef.statuses and "Status" in attrs:
            status_value = attrs["Status"]
            if status_value not in typedef.statuses:
                # Check if we have a deterministic correction
                correction = self._find_status_correction(
                    typedef.name, status_value, typedef.statuses
                )
                issues.append(Issue(
                    kind="invalid_status",
                    severity=_SEVERITY["invalid_status"],
                    page_title=title,
                    detail=f"'{status_value}' is not a valid status (valid: {', '.join(typedef.statuses)})",
                    meta={
                        "current_value": status_value,
                        "valid_statuses": list(typedef.statuses),
                        "suggested_correction": correction,
                        "block_uid": self._find_block_uid(blocks, "Status::"),
                    },
                ))

        # --- Link checks ---

        # Broken links in Related:: attribute (LINK001)
        # Roam strips [[...]] brackets when a referenced page is deleted,
        # leaving bare text. Detect bare text in Related:: as broken links.
        if "Related" in attrs:
            related_value = attrs["Related"]
            bare_text = re.sub(r"\[\[[^\]]+\]\]", "", related_value).strip()
            if bare_text:
                issues.append(Issue(
                    kind="broken_link",
                    severity=_SEVERITY["broken_link"],
                    page_title=title,
                    detail=f"Related:: contains bare text (likely deleted page): '{bare_text}'",
                    meta={
                        "bare_text": bare_text,
                        "block_uid": self._find_block_uid(blocks, "Related::"),
                        "current_value": related_value,
                    },
                ))

        # Orphan detection (ORPHAN001)
        if not self._has_incoming_refs(title):
            issues.append(Issue(
                kind="orphan",
                severity=_SEVERITY["orphan"],
                page_title=title,
                detail="No incoming references from other pages",
            ))

        # --- Semantic checks ---

        # Floating task: Task/ page without Project:: link (SEM004)
        if typedef.name == "Task" and "Project" not in attrs:
            issues.append(Issue(
                kind="floating_task",
                severity=_SEVERITY["floating_task"],
                page_title=title,
                detail="Task has no Project:: attribute",
            ))

        return issues

    def scan_duplicates(self, namespace: str) -> list[Issue]:
        """Scan a namespace for duplicate page names (DUP001).

        Returns list of duplicate issues.
        """
        groups = self._resolver.scan_duplicates(namespace)
        issues = []
        for group in groups:
            titles = [p["title"] for p in group]
            for title in titles:
                issues.append(Issue(
                    kind="duplicate",
                    severity=_SEVERITY["duplicate"],
                    page_title=title,
                    detail=f"Duplicate name group: {', '.join(titles)}",
                    meta={"group": titles},
                ))
        return issues

    def scan_wrong_namespaces(self) -> list[Issue]:
        """Scan for pages using incorrect namespace prefixes (FM002).

        Checks against NAMESPACE_CORRECTIONS for common typos/plurals.
        """
        issues = []
        for wrong_ns, correct_ns in NAMESPACE_CORRECTIONS.items():
            escaped = wrong_ns.replace('"', '\\"')
            results = self._client.q(
                '[:find ?title :where '
                '[?p :node/title ?title] '
                '[(clojure.string/starts-with? ?title "' + escaped + '")]]'
            )
            for (title,) in results:
                name_part = title[len(wrong_ns):]
                issues.append(Issue(
                    kind="wrong_namespace",
                    severity=_SEVERITY["wrong_namespace"],
                    page_title=title,
                    detail=f"Should be '{correct_ns}{name_part}' ('{wrong_ns}' → '{correct_ns}')",
                    meta={
                        "current_namespace": wrong_ns,
                        "correct_namespace": correct_ns,
                        "name_part": name_part,
                    },
                ))
        return issues

    def scan_namespace(
        self, namespace: str, type_name: str | None = None
    ) -> dict[str, list[Issue]]:
        """Scan all pages under a namespace prefix.

        Args:
            namespace: The prefix to match (e.g. "Person/" or "Test/ScanNS ").
            type_name: Explicit type name to validate against.

        Returns:
            Dict mapping page title -> list of issues.
        """
        escaped = namespace.replace('"', '\\"')
        results = self._client.q(
            '[:find ?title :where '
            '[?p :node/title ?title] '
            '[(clojure.string/starts-with? ?title "' + escaped + '")]]'
        )

        report: dict[str, list[Issue]] = {}
        for (title,) in results:
            issues = self.validate_page(title, type_name=type_name)
            report[title] = issues

        return report

    def scan_all(self) -> dict[str, list[Issue]]:
        """Scan all pages under every typed namespace.

        Returns:
            Dict mapping page title -> list of issues.
        """
        types = self._get_types()
        report: dict[str, list[Issue]] = {}
        for td in types.values():
            if td.namespace:
                ns_report = self.scan_namespace(td.namespace, type_name=td.name)
                report.update(ns_report)

                # Add duplicate issues for this namespace
                dup_issues = self.scan_duplicates(td.namespace)
                for issue in dup_issues:
                    report.setdefault(issue.page_title, []).append(issue)

        # Wrong namespace scan (runs once across all pages)
        for issue in self.scan_wrong_namespaces():
            report.setdefault(issue.page_title, []).append(issue)

        return report

    @staticmethod
    def _find_block_uid(blocks: list[dict], prefix: str) -> str | None:
        """Find the UID of a block starting with a given prefix."""
        for b in blocks:
            if b["string"].startswith(prefix):
                return b["uid"]
        return None

    @staticmethod
    def _find_status_correction(
        type_name: str, bad_status: str, valid_statuses: list[str]
    ) -> str | None:
        """Find a deterministic correction for an invalid status value.

        Checks STATUS_CORRECTIONS mapping (type-specific first, then universal),
        then tries case-insensitive match against valid statuses.
        """
        lowered = bad_status.strip().lower()

        # Check type-specific correction first
        correction = STATUS_CORRECTIONS.get((type_name, lowered))
        if correction and correction in valid_statuses:
            return correction

        # Check universal correction
        correction = STATUS_CORRECTIONS.get((None, lowered))
        if correction and correction in valid_statuses:
            return correction

        # Try case-insensitive match against valid statuses
        for valid in valid_statuses:
            if valid.lower() == lowered:
                return valid

        # Try prefix match (e.g., "act" → "active")
        for valid in valid_statuses:
            if valid.lower().startswith(lowered) or lowered.startswith(valid.lower()):
                return valid

        return None
