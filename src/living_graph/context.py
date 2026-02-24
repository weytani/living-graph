# ABOUTME: Builds a compact text snapshot of all typed pages in the Roam graph.
# ABOUTME: Used as context for LLM prompts so the model knows what entities exist.

from __future__ import annotations

from living_graph.ontology import OntologyParser


class GraphContext:
    """Builds a compact graph context snapshot for LLM prompts."""

    def __init__(self, roam):
        self._roam = roam
        self._parser = OntologyParser(roam)

    def build(self) -> str:
        """Build a text snapshot of all typed pages grouped by namespace.

        Format:
            ## Person/ (26 pages)
            - Person/Shane Parrish
            - Person/David Allen
            ...

            ## Project/ (13 pages)
            - Project/Marathon
            ...
        """
        types = self._parser.parse()
        sections = []

        for typedef in sorted(types.values(), key=lambda t: t.namespace):
            pages = self._roam.q(
                '[:find ?title :where '
                '[?p :node/title ?title] '
                '[(clojure.string/starts-with? ?title "'
                + typedef.namespace
                + '")]]'
            )
            if not pages:
                continue

            titles = sorted(row[0] for row in pages)
            header = f"## {typedef.namespace} ({len(titles)} page{'s' if len(titles) != 1 else ''})"
            listing = "\n".join(f"- {t}" for t in titles)
            sections.append(f"{header}\n{listing}")

        return "\n\n".join(sections)
