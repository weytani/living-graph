# ABOUTME: Tests for entity resolution — dedup-on-create and retroactive scanning.
# ABOUTME: Uses Test/ namespace pages to verify matching without polluting real data.

import time
from living_graph.entity_resolution import EntityResolver


def test_exact_match_finds_existing(roam, test_namespace):
    roam.create_page("Test/Entity Match")
    time.sleep(2)
    resolver = EntityResolver(roam)
    result = resolver.resolve("Test/", "Entity Match")
    assert result is not None
    assert result["title"] == "Test/Entity Match"
    assert "uid" in result


def test_case_insensitive_match(roam, test_namespace):
    roam.create_page("Test/Case Test")
    time.sleep(2)
    resolver = EntityResolver(roam)
    result = resolver.resolve("Test/", "case test")
    assert result is not None
    assert result["title"] == "Test/Case Test"


def test_no_match_returns_none(roam, test_namespace):
    resolver = EntityResolver(roam)
    result = resolver.resolve("Test/", "Nonexistent Page XYZ123")
    assert result is None


def test_normalize_title(roam):
    resolver = EntityResolver(roam)
    assert resolver.normalize("  shane parrish  ") == "Shane Parrish"
    assert resolver.normalize("LOUD TITLE") == "Loud Title"
    assert resolver.normalize("already Good") == "Already Good"


def test_retroactive_scan_finds_duplicates(roam, test_namespace):
    roam.create_page("Test/Duplicate One")
    roam.create_page("Test/duplicate one")
    time.sleep(2)
    resolver = EntityResolver(roam)
    dupes = resolver.scan_duplicates("Test/")
    assert len(dupes) > 0
    titles = set()
    for group in dupes:
        for entry in group:
            titles.add(entry["title"])
    assert "Test/Duplicate One" in titles or "Test/duplicate one" in titles


def test_fuzzy_match(roam, test_namespace):
    roam.create_page("Test/Shane Parrish")
    time.sleep(2)
    resolver = EntityResolver(roam)
    matches = resolver.fuzzy_match("Test/", "Shaun Parish", threshold=0.7)
    assert len(matches) > 0
    assert any("Shane Parrish" in m["title"] for m in matches)


def test_resolve_or_create_existing(roam, test_namespace):
    roam.create_page("Test/Existing Entity")
    time.sleep(2)
    resolver = EntityResolver(roam)
    result = resolver.resolve_or_create("Test/", "existing entity")
    assert result["title"] == "Test/Existing Entity"
    assert result["created"] is False


def test_resolve_or_create_new(roam, test_namespace):
    resolver = EntityResolver(roam)
    result = resolver.resolve_or_create("Test/", "Brand New Entity")
    assert result["title"] == "Test/Brand New Entity"
    assert result["created"] is True
