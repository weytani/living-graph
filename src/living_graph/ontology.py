# ABOUTME: Parser for Convention/Ontology page in Roam.
# ABOUTME: Extracts TypeDef objects (namespace, statuses, required attrs) from the hierarchical block tree.

from __future__ import annotations

import re
from dataclasses import dataclass, field


@dataclass
class TypeDef:
    """A single type definition parsed from Convention/Ontology."""

    name: str
    namespace: str
    statuses: list[str] = field(default_factory=list)
    required: list[str] = field(default_factory=list)
    notes: str = ""
    example: str = ""


class OntologyParser:
    """Parses the Convention/Ontology page into TypeDef objects."""

    PAGE_TITLE = "Convention/Ontology"
    PULL_SELECTOR = "[:block/uid :block/string :block/order {:block/children ...}]"

    def __init__(self, roam):
        self._roam = roam
        self._types: dict[str, TypeDef] | None = None

    def parse(self) -> dict[str, TypeDef]:
        """Parse the ontology page and return a dict of type name -> TypeDef.

        Results are cached — subsequent calls return the same dict.
        """
        if self._types is not None:
            return self._types

        # Find the page UID
        results = self._roam.q(
            '[:find ?uid :where '
            '[?p :node/title "Convention/Ontology"] '
            '[?p :block/uid ?uid]]'
        )
        if not results:
            raise RuntimeError("Convention/Ontology page not found")

        page_uid = results[0][0]

        # Pull the full block tree
        tree = self._roam.pull(
            self.PULL_SELECTOR, f'[:block/uid "{page_uid}"]'
        )

        # Walk the tree and collect type definitions
        self._types = {}
        self._walk(tree)
        return self._types

    def type_for_namespace(self, prefix: str) -> TypeDef | None:
        """Look up a TypeDef by its namespace prefix (e.g. 'Person/')."""
        types = self.parse()
        for td in types.values():
            if td.namespace == prefix:
                return td
        return None

    # --- Private helpers ---

    def _walk(self, node: dict) -> None:
        """Recursively walk the block tree looking for type definition blocks."""
        string = node.get(":block/string", "")
        children = sorted(
            node.get(":block/children", []),
            key=lambda b: b.get(":block/order", 0),
        )

        # Check if this is a type definition block: **TypeName** with Namespace:: child
        match = re.match(r"^\*\*(.+)\*\*$", string.strip())
        if match and children:
            has_namespace = any(
                "Namespace::" in c.get(":block/string", "") for c in children
            )
            if has_namespace:
                type_name = match.group(1)
                typedef = self._parse_type_block(type_name, children)
                self._types[type_name] = typedef
                return  # Don't recurse into type block children

        # Recurse into children
        for child in children:
            self._walk(child)

    def _parse_type_block(self, name: str, children: list[dict]) -> TypeDef:
        """Parse a type definition block's children into a TypeDef."""
        namespace = ""
        statuses: list[str] = []
        required: list[str] = []
        notes = ""
        example = ""

        for child in children:
            text = child.get(":block/string", "")

            if text.startswith("Namespace::"):
                namespace = self._parse_inline_code(text)

            elif text.startswith("Statuses::"):
                statuses = self._parse_statuses(text)

            elif text.startswith("Required::"):
                required = self._parse_required(text)

            elif text.startswith("Notes::"):
                notes = text.split("::", 1)[1].strip()

            elif text.startswith("Example::"):
                example = self._parse_inline_code(text)

        return TypeDef(
            name=name,
            namespace=namespace,
            statuses=statuses,
            required=required,
            notes=notes,
            example=example,
        )

    @staticmethod
    def _parse_inline_code(text: str) -> str:
        """Extract the first backtick-wrapped value from text."""
        m = re.search(r"`([^`]+)`", text)
        return m.group(1) if m else text.split("::", 1)[1].strip()

    @staticmethod
    def _parse_statuses(text: str) -> list[str]:
        """Parse the Statuses:: field into a list of status strings."""
        value = text.split("::", 1)[1].strip()

        # (none ...) or (none) -> empty list
        if value.startswith("("):
            return []

        # Comma-separated values
        return [s.strip() for s in value.split(",") if s.strip()]

    @staticmethod
    def _parse_required(text: str) -> list[str]:
        """Parse the Required:: field into a list of attribute names.

        Values are in backtick code format like `Role::`, `Status::`.
        Extract the name, strip the :: suffix.
        Some have parenthetical notes like `Type::` (employer/client/community).
        """
        # Extract all backtick-wrapped values
        matches = re.findall(r"`([^`]+)`", text)

        if matches:
            attrs = []
            for m in matches:
                # Strip trailing :: if present
                name = m.rstrip(":")
                # Also strip trailing :: that may be part of the name
                if name.endswith(":"):
                    name = name.rstrip(":")
                attrs.append(name.strip())
            return attrs

        # Fallback: split by comma after the ::
        value = text.split("::", 1)[1].strip()
        return [s.strip().rstrip(":") for s in value.split(",") if s.strip()]
