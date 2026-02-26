# ABOUTME: Ollama embedding client and Roam page text extraction.
# ABOUTME: Converts page content to vectors for semantic clustering.

from __future__ import annotations

import httpx

from living_graph.client import RoamClient


DEFAULT_MODEL = "nomic-embed-text"
DEFAULT_BASE_URL = "http://localhost:11434"


class OllamaEmbedder:
    """Client for generating embeddings via Ollama."""

    def __init__(
        self,
        model: str = DEFAULT_MODEL,
        base_url: str = DEFAULT_BASE_URL,
    ):
        self._model = model
        self._base_url = base_url

    def embed(self, text: str) -> list[float]:
        """Embed a single text string. Returns a float vector.

        Empty strings are replaced with a single space so Ollama
        produces a valid (if semantically meaningless) vector.
        """
        if not text:
            text = " "
        resp = httpx.post(
            f"{self._base_url}/api/embed",
            json={"model": self._model, "input": text},
            timeout=60.0,
        )
        resp.raise_for_status()
        data = resp.json()
        return data["embeddings"][0]

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """Embed multiple texts in a single request."""
        resp = httpx.post(
            f"{self._base_url}/api/embed",
            json={"model": self._model, "input": texts},
            timeout=120.0,
        )
        resp.raise_for_status()
        data = resp.json()
        return data["embeddings"]


def extract_page_text(roam: RoamClient, page_title: str) -> str:
    """Extract title + all block text from a Roam page.

    Returns a single string: "Title\\nblock1\\nblock2\\n..."
    Returns empty string if page doesn't exist.
    """
    results = roam.q(
        f'[:find ?uid :where [?p :node/title "{page_title}"] [?p :block/uid ?uid]]'
    )
    if not results:
        return ""

    page_uid = results[0][0]
    tree = roam.pull(
        "[:block/uid {:block/children [:block/string :block/order {:block/children ...}]}]",
        f'[:block/uid "{page_uid}"]',
    )

    lines = [page_title]
    _collect_text(tree, lines)
    return "\n".join(lines)


def _collect_text(node: dict, lines: list[str]) -> None:
    """Recursively collect block text from a Roam pull tree."""
    children = sorted(
        node.get(":block/children", []),
        key=lambda b: b.get(":block/order", 0),
    )
    for child in children:
        text = child.get(":block/string", "").strip()
        if text:
            lines.append(text)
        _collect_text(child, lines)
