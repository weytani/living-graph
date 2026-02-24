# ABOUTME: Interlinking helpers for bidirectional Related:: wiring between pages.
# ABOUTME: Finds unlinked references and manages Related:: attribute blocks.

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from living_graph.client import RoamClient


class Interlinker:
    """Manages Related:: links between Roam pages."""

    def __init__(self, roam: RoamClient) -> None:
        self._roam = roam

    def find_unlinked_references(self, page_title: str) -> list[dict]:
        """Find pages that reference *page_title* but aren't in its Related:: block.

        Returns list of ``{"uid": str, "title": str}`` dicts for each
        referencing page that is not already listed in the target page's
        Related:: attribute.
        """
        query = (
            "[:find ?page-uid ?page-title :where "
            f'[?ref-page :node/title "{page_title}"] '
            "[?b :block/refs ?ref-page] "
            "[?b :block/page ?source-page] "
            "[?source-page :block/uid ?page-uid] "
            "[?source-page :node/title ?page-title]]"
        )
        results = self._roam.q(query)

        # Filter out self-references
        refs = [
            {"uid": uid, "title": title}
            for uid, title in results
            if title != page_title
        ]

        # Filter out pages already in the Related:: block
        already_related = self._get_related_titles(page_title)
        return [r for r in refs if r["title"] not in already_related]

    def _get_related_titles(self, page_title: str) -> set[str]:
        """Extract titles from the Related:: block of *page_title*."""
        page_uid_results = self._roam.q(
            f'[:find ?uid :where [?p :node/title "{page_title}"] [?p :block/uid ?uid]]'
        )
        if not page_uid_results:
            return set()
        page_uid = page_uid_results[0][0]
        _, text = self._find_related_block(page_uid)
        return set(re.findall(r"\[\[([^\]]+)\]\]", text))

    def _find_related_block(self, page_uid: str) -> tuple[str | None, str]:
        """Find the Related:: block under a page.

        Returns ``(block_uid, block_text)`` or ``(None, "")`` if none exists.
        """
        tree = self._roam.pull(
            "[:block/uid {:block/children [:block/uid :block/string]}]",
            f'[:block/uid "{page_uid}"]',
        )
        for child in tree.get(":block/children", []):
            text = child.get(":block/string", "")
            if text.startswith("Related::"):
                return child[":block/uid"], text
        return None, ""

    def add_related(self, page_uid: str, titles: list[str]) -> None:
        """Add *titles* to the Related:: block of *page_uid*.

        If a Related:: block already exists, appends only titles that are
        not already present.  Otherwise creates a new Related:: block.
        """
        block_uid, existing_text = self._find_related_block(page_uid)

        if block_uid is not None:
            # Parse existing links to avoid duplicates
            existing_links = set(re.findall(r"\[\[([^\]]+)\]\]", existing_text))
            new_titles = [t for t in titles if t not in existing_links]
            if not new_titles:
                return
            new_links = " ".join(f"[[{t}]]" for t in new_titles)
            updated = f"{existing_text} {new_links}"
            self._roam.update_block(block_uid, updated)
        else:
            links = " ".join(f"[[{t}]]" for t in titles)
            block_string = f"Related:: {links}"
            self._roam.create_block(page_uid, block_string, 0)

    def link_bidirectional(
        self,
        uid_a: str,
        title_a: str,
        uid_b: str,
        title_b: str,
    ) -> None:
        """Wire Related:: links in both directions between two pages."""
        self.add_related(uid_a, [title_b])
        self.add_related(uid_b, [title_a])
