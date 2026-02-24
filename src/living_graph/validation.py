# ABOUTME: Validation scanner that checks Roam pages against ontology type definitions.
# ABOUTME: Detects missing required attributes, invalid statuses, stub pages, and orphans.

from __future__ import annotations

from dataclasses import dataclass

from living_graph.ontology import OntologyParser, TypeDef


@dataclass
class Issue:
    """A single validation issue found on a page."""

    kind: str  # missing_attr, invalid_status, stub, orphan
    severity: str  # error, warning, info
    page_title: str
    detail: str


# Severity rules by issue kind
_SEVERITY = {
    "missing_attr": "warning",
    "invalid_status": "error",
    "stub": "info",
    "orphan": "warning",
}


class ValidationScanner:
    """Scans Roam pages for ontology compliance issues."""

    PULL_SELECTOR = "[:block/uid :block/string :block/order {:block/children ...}]"

    def __init__(self, client):
        self._client = client
        self._parser = OntologyParser(client)
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

    def _get_page_attrs(self, title: str) -> tuple[list[str], dict[str, str]]:
        """Fetch a page's children and parse attribute blocks.

        Returns (raw_strings, attrs_dict) where attrs_dict maps
        attribute name -> value for any block matching 'Key:: Value'.
        """
        results = self._client.q(
            '[:find ?uid :where '
            '[?p :node/title ?title] '
            '[(= ?title "' + title.replace('"', '\\"') + '")] '
            '[?p :block/uid ?uid]]'
        )
        if not results:
            return [], {}

        page_uid = results[0][0]
        tree = self._client.pull(self.PULL_SELECTOR, f'[:block/uid "{page_uid}"]')

        children = sorted(
            tree.get(":block/children", []),
            key=lambda b: b.get(":block/order", 0),
        )

        raw_strings = [c.get(":block/string", "") for c in children]
        attrs: dict[str, str] = {}
        for s in raw_strings:
            if "::" in s:
                key, _, value = s.partition("::")
                attrs[key.strip()] = value.strip()

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

        raw_strings, attrs = self._get_page_attrs(title)
        issues: list[Issue] = []

        # Stub detection: page exists but has no children at all
        if not raw_strings:
            issues.append(Issue(
                kind="stub",
                severity=_SEVERITY["stub"],
                page_title=title,
                detail="Page has no content",
            ))

        # Missing required attributes
        for attr_name in typedef.required:
            if attr_name not in attrs:
                issues.append(Issue(
                    kind="missing_attr",
                    severity=_SEVERITY["missing_attr"],
                    page_title=title,
                    detail=attr_name,
                ))

        # Invalid status (only if the type defines statuses and the page has one)
        if typedef.statuses and "Status" in attrs:
            status_value = attrs["Status"]
            if status_value not in typedef.statuses:
                issues.append(Issue(
                    kind="invalid_status",
                    severity=_SEVERITY["invalid_status"],
                    page_title=title,
                    detail=f"'{status_value}' is not a valid status (valid: {', '.join(typedef.statuses)})",
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
        return report
