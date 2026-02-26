# ABOUTME: Tests for the embeddings module — Ollama integration.
# ABOUTME: Verifies text extraction from Roam pages and embedding generation.

import time
import pytest
from living_graph.embeddings import OllamaEmbedder, extract_page_text


def test_extract_page_text(roam):
    """Should extract title + block text from a Roam page."""
    roam.create_page("Test/Embed Page")
    time.sleep(2)
    pages = roam.q(
        '[:find ?uid :where [?p :node/title "Test/Embed Page"] [?p :block/uid ?uid]]'
    )
    page_uid = pages[0][0]
    roam.create_block(page_uid, "First block about Python", 0)
    roam.create_block(page_uid, "Second block about testing", 1)
    time.sleep(2)

    text = extract_page_text(roam, "Test/Embed Page")
    assert "Test/Embed Page" in text
    assert "Python" in text
    assert "testing" in text


def test_extract_page_text_nonexistent(roam):
    """Should return empty string for nonexistent page."""
    text = extract_page_text(roam, "Test/Nonexistent 99999")
    assert text == ""


def test_embed_single_text():
    """Should return a list of floats from Ollama."""
    embedder = OllamaEmbedder()
    vector = embedder.embed("Hello world, this is a test.")
    assert isinstance(vector, list)
    assert len(vector) > 0
    assert isinstance(vector[0], float)


def test_embed_batch():
    """Should embed multiple texts and return matching-length result."""
    embedder = OllamaEmbedder()
    texts = ["First text about cats", "Second text about dogs"]
    vectors = embedder.embed_batch(texts)
    assert len(vectors) == 2
    assert len(vectors[0]) == len(vectors[1])  # Same dimensionality


def test_embed_empty_text():
    """Should handle empty string gracefully."""
    embedder = OllamaEmbedder()
    vector = embedder.embed("")
    assert isinstance(vector, list)
    assert len(vector) > 0
