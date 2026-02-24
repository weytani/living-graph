# ABOUTME: Entity resolution for Roam pages — dedup-on-create and retroactive scanning.
# ABOUTME: Matches by exact, case-insensitive, and fuzzy title within a namespace.

from __future__ import annotations

import difflib
import time


class EntityResolver:
    """Resolve entity pages within a Roam namespace to prevent duplicates."""

    def __init__(self, roam):
        self._roam = roam

    def _pages_in_namespace(self, namespace: str) -> list[dict]:
        """Fetch all pages whose title starts with the given namespace prefix."""
        results = self._roam.q(
            '[:find ?uid ?title :where '
            '[?p :node/title ?title] '
            '[?p :block/uid ?uid] '
            '[(clojure.string/starts-with? ?title "' + namespace + '")]]'
        )
        return [{"uid": r[0], "title": r[1]} for r in results]

    def resolve(self, namespace: str, name: str) -> dict | None:
        """Find an existing page matching name (case-insensitive) in namespace.

        Returns {"uid": str, "title": str} or None.
        """
        pages = self._pages_in_namespace(namespace)
        target = name.lower()
        for page in pages:
            # Strip the namespace prefix, compare the name part
            page_name = page["title"][len(namespace):]
            if page_name.lower() == target:
                return page
        return None

    def normalize(self, title: str) -> str:
        """Normalize a title: strip whitespace, convert to title case."""
        return title.strip().title()

    def fuzzy_match(
        self, namespace: str, name: str, threshold: float = 0.7
    ) -> list[dict]:
        """Find pages in namespace that fuzzy-match name above threshold.

        Returns list of {"uid", "title", "score"} sorted by score descending.
        """
        pages = self._pages_in_namespace(namespace)
        target = name.lower()
        matches = []
        for page in pages:
            page_name = page["title"][len(namespace):]
            score = difflib.SequenceMatcher(
                None, target, page_name.lower()
            ).ratio()
            if score >= threshold:
                matches.append({
                    "uid": page["uid"],
                    "title": page["title"],
                    "score": score,
                })
        matches.sort(key=lambda m: m["score"], reverse=True)
        return matches

    def scan_duplicates(self, namespace: str) -> list[list[dict]]:
        """Find groups of pages with duplicate names (case-insensitive).

        Returns list of groups, where each group is a list of
        {"uid", "title"} with len > 1.
        """
        pages = self._pages_in_namespace(namespace)
        groups: dict[str, list[dict]] = {}
        for page in pages:
            page_name = page["title"][len(namespace):].lower()
            groups.setdefault(page_name, []).append(page)
        return [group for group in groups.values() if len(group) > 1]

    def resolve_or_create(
        self, namespace: str, name: str
    ) -> dict:
        """Resolve an existing page or create a new one.

        Returns {"uid": str, "title": str, "created": bool}.
        """
        existing = self.resolve(namespace, name)
        if existing:
            return {**existing, "created": False}

        # Normalize and create
        normalized = self.normalize(name)
        full_title = namespace + normalized
        self._roam.create_page(full_title)
        time.sleep(2)

        # Query back for the UID
        results = self._roam.q(
            '[:find ?uid :where '
            '[?p :node/title "' + full_title + '"] '
            '[?p :block/uid ?uid]]'
        )
        if not results:
            raise RuntimeError(f"Created page '{full_title}' but could not find its UID")

        uid = results[0][0]
        return {"uid": uid, "title": full_title, "created": True}
