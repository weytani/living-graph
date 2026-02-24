# Phase 2: Deterministic Layer — Implementation Plan

> **For Claude:** Use `@skills/collaboration/executing-plans/SKILL.md` to implement this plan task-by-task.

**Goal:** Build a Python library that handles entity resolution, validation, interlinking, and mutation logging against the Roam graph — the non-LLM engine that all four workers will use.

**Architecture:** Flat module library (`living_graph/`) with a thin Roam API client. Each module is independent — no module imports another except `client` and `ontology`. Tests hit the real Roam API using a `Test/` namespace for isolation.

**Tech Stack:** Python 3.14, requests, pytest. Roam Backend API (Datalog queries, REST writes). No mocks — real API calls in tests.

**Roam API:** `https://api.roamresearch.com` — bearer token auth, 3 read endpoints (`/q`, `/pull`, `/pull-many`), 1 write endpoint (`/write`) with 8 action types. Same Datalog as MCP.

---

## Task 0: Project Setup

**Files:**
- Create: `~/code/living-graph/pyproject.toml`
- Create: `~/code/living-graph/src/living_graph/__init__.py`
- Create: `~/code/living-graph/src/living_graph/client.py`
- Create: `~/code/living-graph/tests/__init__.py`
- Create: `~/code/living-graph/tests/conftest.py`
- Create: `~/code/living-graph/.env.example`
- Create: `~/code/living-graph/.gitignore`

**Step 1: Initialize the project**

```bash
cd ~/code/living-graph
git init
```

**Step 2: Create pyproject.toml**

```toml
[project]
name = "living-graph"
version = "0.1.0"
description = "Deterministic layer for Roam living graph maintenance"
requires-python = ">=3.13"
dependencies = [
    "requests>=2.31",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "python-dotenv>=1.0",
]

[build-system]
requires = ["setuptools>=69.0"]
build-backend = "setuptools.backends._legacy:_Backend"

[tool.setuptools.packages.find]
where = ["src"]

[tool.pytest.ini_options]
testpaths = ["tests"]
```

**Step 3: Create .gitignore**

```
.env
__pycache__/
*.pyc
.venv/
dist/
*.egg-info/
```

**Step 4: Create .env.example**

```
ROAM_GRAPH=your-graph-name
ROAM_API_TOKEN=roam-graph-token-xxx
```

**Step 5: Set up venv and install**

```bash
cd ~/code/living-graph
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

**Step 6: Create conftest.py with shared fixtures**

```python
# ABOUTME: Shared pytest fixtures for Roam API integration tests.
# ABOUTME: Provides authenticated client and Test/ namespace cleanup.

import os
import pytest
from dotenv import load_dotenv
from living_graph.client import RoamClient

load_dotenv()


@pytest.fixture(scope="session")
def roam():
    """Authenticated Roam client for the session."""
    graph = os.environ["ROAM_GRAPH"]
    token = os.environ["ROAM_API_TOKEN"]
    return RoamClient(graph, token)


@pytest.fixture(scope="session")
def test_namespace():
    """Namespace prefix for test artifacts."""
    return "Test/"


@pytest.fixture(autouse=True)
def cleanup_test_pages(roam, test_namespace):
    """Delete any Test/ pages created during a test."""
    yield
    # Find all Test/ pages
    results = roam.q(
        '[:find ?uid ?title :where [?p :node/title ?title] [?p :block/uid ?uid] [(clojure.string/starts-with? ?title "Test/")]]'
    )
    for uid, title in results:
        roam.delete_page(uid)
```

**Step 7: Create .env with real credentials**

The user must fill in `ROAM_GRAPH` and `ROAM_API_TOKEN` from Roam Settings > Graph > API Tokens. Create a token with `edit` scope.

```bash
cp .env.example .env
# Edit .env with real values
```

**Step 8: Commit**

```bash
git add pyproject.toml src/ tests/ .gitignore .env.example
git commit -m "chore: initialize living-graph project"
```

---

## Task 1: Roam API Client

**Files:**
- Create: `src/living_graph/client.py`
- Create: `tests/test_client.py`

### Step 1: Write the failing test

```python
# ABOUTME: Integration tests for the Roam API client.
# ABOUTME: Validates query, pull, write, and rate-limit handling against real graph.

import time


def test_query_returns_results(roam):
    """Basic Datalog query returns page titles."""
    results = roam.q("[:find ?title :where [?p :node/title ?title] :limit 5]")
    assert isinstance(results, list)
    assert len(results) > 0
    assert isinstance(results[0], list)


def test_pull_returns_entity(roam):
    """Pull a known page by title."""
    result = roam.pull("[*]", '[:node/title "Convention/Ontology"]')
    assert result is not None
    assert "block/uid" in result or ":block/uid" in result


def test_create_and_delete_page(roam):
    """Create a Test/ page, verify it exists, delete it."""
    roam.create_page("Test/Client Integration")
    results = roam.q(
        '[:find ?uid :where [?p :node/title "Test/Client Integration"] [?p :block/uid ?uid]]'
    )
    assert len(results) == 1
    uid = results[0][0]
    roam.delete_page(uid)
    # Verify deletion
    time.sleep(1)
    results = roam.q(
        '[:find ?uid :where [?p :node/title "Test/Client Integration"] [?p :block/uid ?uid]]'
    )
    assert len(results) == 0


def test_create_and_delete_block(roam):
    """Create a block under a Test/ page, verify, cleanup."""
    roam.create_page("Test/Block Test")
    pages = roam.q(
        '[:find ?uid :where [?p :node/title "Test/Block Test"] [?p :block/uid ?uid]]'
    )
    page_uid = pages[0][0]

    roam.create_block(parent_uid=page_uid, string="test block content", order=0)
    time.sleep(1)
    children = roam.pull(
        "[:block/children {:block/children [:block/string :block/uid]}]",
        f'[:block/uid "{page_uid}"]',
    )
    child_strings = [
        c.get(":block/string", c.get("block/string", ""))
        for c in children.get(":block/children", children.get("block/children", []))
    ]
    assert "test block content" in child_strings

    roam.delete_page(page_uid)


def test_batch_actions(roam):
    """Batch multiple operations in one call."""
    roam.create_page("Test/Batch Test")
    pages = roam.q(
        '[:find ?uid :where [?p :node/title "Test/Batch Test"] [?p :block/uid ?uid]]'
    )
    page_uid = pages[0][0]

    roam.batch([
        {
            "action": "create-block",
            "location": {"parent-uid": page_uid, "order": 0},
            "block": {"string": "batch block 1"},
        },
        {
            "action": "create-block",
            "location": {"parent-uid": page_uid, "order": 1},
            "block": {"string": "batch block 2"},
        },
    ])

    time.sleep(1)
    children = roam.pull(
        "[:block/children {:block/children [:block/string]}]",
        f'[:block/uid "{page_uid}"]',
    )
    strings = [
        c.get(":block/string", c.get("block/string", ""))
        for c in children.get(":block/children", children.get("block/children", []))
    ]
    assert "batch block 1" in strings
    assert "batch block 2" in strings

    roam.delete_page(page_uid)


def test_rate_limit_retry(roam):
    """Verify client handles 429 responses gracefully."""
    # Rapid-fire queries to trigger rate limiting
    for _ in range(15):
        results = roam.q("[:find ?title :where [?p :node/title ?title] :limit 1]")
        assert isinstance(results, list)
```

### Step 2: Run tests to verify they fail

```bash
cd ~/code/living-graph
source .venv/bin/activate
pytest tests/test_client.py -v
```

Expected: FAIL — `living_graph.client` has no `RoamClient` class yet.

### Step 3: Write minimal implementation

```python
# ABOUTME: Thin wrapper around the Roam Research Backend API.
# ABOUTME: Handles auth, rate limiting, and provides typed methods for query/write ops.

import time
import requests


class RoamClient:
    """Authenticated client for the Roam Backend API."""

    BASE_URL = "https://api.roamresearch.com"

    def __init__(self, graph: str, token: str, max_retries: int = 3):
        self.graph = graph
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Content-Type": "application/json; charset=utf-8",
                "Authorization": f"Bearer {token}",
                "x-authorization": f"Bearer {token}",
            }
        )
        self._max_retries = max_retries
        # Cache redirect target per Roam API docs
        self._base_url = self.BASE_URL

    def _url(self, endpoint: str) -> str:
        return f"{self._base_url}/api/graph/{self.graph}/{endpoint}"

    def _request(self, endpoint: str, payload: dict) -> dict:
        """Make a request with retry on 429."""
        for attempt in range(self._max_retries + 1):
            resp = self._session.post(self._url(endpoint), json=payload)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                wait = min(2 ** attempt * 5, 65)
                time.sleep(wait)
                continue
            # Handle Roam's peer redirect
            if resp.status_code in (301, 302, 307, 308):
                self._base_url = resp.headers.get("Location", self._base_url).rsplit(
                    "/api", 1
                )[0]
                continue
            resp.raise_for_status()
        raise RuntimeError(f"Rate limited after {self._max_retries} retries")

    # --- Read operations ---

    def q(self, query: str, args: list | None = None) -> list:
        """Execute a Datalog query. Returns list of result tuples."""
        payload = {"query": query}
        if args:
            payload["args"] = args
        result = self._request("q", payload)
        return result.get("result", result)

    def pull(self, selector: str, eid: str) -> dict:
        """Pull an entity by selector and entity ID."""
        return self._request("pull", {"selector": selector, "eid": eid}).get(
            "result", {}
        )

    def pull_many(self, selector: str, eids: list[str]) -> list[dict]:
        """Pull multiple entities."""
        return self._request(
            "pull-many", {"selector": selector, "eids": eids}
        ).get("result", [])

    # --- Write operations ---

    def _write(self, payload: dict) -> dict:
        return self._request("write", payload)

    def create_page(self, title: str, uid: str | None = None) -> dict:
        page = {"title": title}
        if uid:
            page["uid"] = uid
        return self._write({"action": "create-page", "page": page})

    def update_page(self, uid: str, title: str | None = None) -> dict:
        page = {"uid": uid}
        if title:
            page["title"] = title
        return self._write({"action": "update-page", "page": page})

    def delete_page(self, uid: str) -> dict:
        return self._write({"action": "delete-page", "page": {"uid": uid}})

    def create_block(
        self, parent_uid: str, string: str, order: int | str = "last"
    ) -> dict:
        return self._write(
            {
                "action": "create-block",
                "location": {"parent-uid": parent_uid, "order": order},
                "block": {"string": string},
            }
        )

    def update_block(self, uid: str, string: str) -> dict:
        return self._write(
            {"action": "update-block", "block": {"uid": uid, "string": string}}
        )

    def delete_block(self, uid: str) -> dict:
        return self._write({"action": "delete-block", "block": {"uid": uid}})

    def batch(self, actions: list[dict]) -> dict:
        return self._write({"action": "batch-actions", "actions": actions})
```

### Step 4: Run tests to verify they pass

```bash
pytest tests/test_client.py -v
```

Expected: All 6 tests PASS.

### Step 5: Commit

```bash
git add src/living_graph/client.py tests/test_client.py
git commit -m "feat: add Roam API client with rate limiting and retry"
```

---

## Task 2: Ontology Parser

**Files:**
- Create: `src/living_graph/ontology.py`
- Create: `tests/test_ontology.py`

The ontology parser reads `Convention/Ontology` from Roam and turns it into Python dataclasses. Every other module needs this to know what "valid" means.

### Step 1: Write the failing test

```python
# ABOUTME: Tests for the ontology parser.
# ABOUTME: Verifies Convention/Ontology page is correctly parsed into TypeDef objects.

from living_graph.ontology import OntologyParser, TypeDef


def test_parse_returns_all_types(roam):
    parser = OntologyParser(roam)
    types = parser.parse()
    assert len(types) == 20
    assert all(isinstance(t, TypeDef) for t in types.values())


def test_person_type_has_correct_fields(roam):
    parser = OntologyParser(roam)
    types = parser.parse()
    person = types["Person"]
    assert person.namespace == "Person/"
    assert "Role" in person.required
    assert "Related" in person.required
    assert person.statuses == []


def test_project_type_has_statuses(roam):
    parser = OntologyParser(roam)
    types = parser.parse()
    project = types["Project"]
    assert "active" in project.statuses
    assert "paused" in project.statuses
    assert "completed" in project.statuses
    assert "archived" in project.statuses
    assert "Track" in project.required
    assert "Status" in project.required


def test_task_type_has_statuses(roam):
    parser = OntologyParser(roam)
    types = parser.parse()
    task = types["Task"]
    assert "todo" in task.statuses
    assert "active" in task.statuses
    assert "blocked" in task.statuses
    assert "done" in task.statuses


def test_type_lookup_by_namespace(roam):
    parser = OntologyParser(roam)
    types = parser.parse()
    # Should be able to find type by namespace prefix
    person = parser.type_for_namespace("Person/")
    assert person is not None
    assert person.name == "Person"


def test_epistemic_types_present(roam):
    parser = OntologyParser(roam)
    types = parser.parse()
    epistemic = ["Assumption", "Constraint", "Contradiction", "Synthesis", "Decision"]
    for name in epistemic:
        assert name in types, f"Missing epistemic type: {name}"
```

### Step 2: Run tests to verify they fail

```bash
pytest tests/test_ontology.py -v
```

Expected: FAIL — `living_graph.ontology` does not exist.

### Step 3: Write minimal implementation

```python
# ABOUTME: Parses Convention/Ontology from Roam into Python dataclasses.
# ABOUTME: Provides TypeDef objects that other modules use for validation and entity resolution.

import re
from dataclasses import dataclass, field
from living_graph.client import RoamClient


@dataclass
class TypeDef:
    """A single type from the ontology."""

    name: str
    namespace: str
    statuses: list[str] = field(default_factory=list)
    required: list[str] = field(default_factory=list)
    notes: str = ""


class OntologyParser:
    """Fetch and parse Convention/Ontology into TypeDef objects."""

    ONTOLOGY_PAGE = "Convention/Ontology"

    def __init__(self, client: RoamClient):
        self._client = client
        self._types: dict[str, TypeDef] | None = None

    def parse(self) -> dict[str, TypeDef]:
        """Parse the ontology page, returning {name: TypeDef}."""
        if self._types is not None:
            return self._types

        # Pull the full page tree
        page = self._client.q(
            '[:find ?uid :where [?p :node/title "Convention/Ontology"] [?p :block/uid ?uid]]'
        )
        if not page:
            raise ValueError("Convention/Ontology page not found")

        page_uid = page[0][0]
        tree = self._client.pull(
            "[:block/uid :block/string :block/order {:block/children ...}]",
            f'[:block/uid "{page_uid}"]',
        )

        self._types = {}
        self._walk_tree(tree)
        return self._types

    def _walk_tree(self, node: dict) -> None:
        """Recursively walk the block tree looking for type definitions."""
        string = node.get(":block/string", node.get("block/string", ""))
        children = node.get(":block/children", node.get("block/children", []))

        # Sort children by order for consistent traversal
        children = sorted(
            children,
            key=lambda c: c.get(":block/order", c.get("block/order", 0)),
        )

        # A type heading is a bold block like **Person** under a tier heading
        bold_match = re.match(r"^\*\*(\w+)\*\*$", string.strip())
        if bold_match:
            name = bold_match.group(1)
            # Check if children contain Namespace:: — that means it's a type def
            typedef = self._parse_type_block(name, children)
            if typedef:
                self._types[name] = typedef

        for child in children:
            self._walk_tree(child)

    def _parse_type_block(self, name: str, children: list[dict]) -> TypeDef | None:
        """Parse a type definition from its child blocks."""
        namespace = ""
        statuses = []
        required = []
        notes = ""

        for child in children:
            text = child.get(":block/string", child.get("block/string", ""))

            if text.startswith("Namespace::"):
                namespace = text.split("::", 1)[1].strip().strip("`")
            elif text.startswith("Statuses::"):
                raw = text.split("::", 1)[1].strip()
                if raw and not raw.startswith("("):
                    statuses = [s.strip() for s in raw.split(",")]
            elif text.startswith("Required::"):
                raw = text.split("::", 1)[1].strip()
                # Extract attribute names from `Attr::` patterns
                required = [
                    m.strip().rstrip(":") for m in re.findall(r"`([^`]+)`", raw)
                ]
                if not required:
                    # Fallback: split by comma and clean
                    required = [
                        s.strip().rstrip("::").rstrip(":")
                        for s in raw.split(",")
                        if s.strip()
                    ]
            elif text.startswith("Notes::"):
                notes = text.split("::", 1)[1].strip()

        if not namespace:
            return None
        return TypeDef(
            name=name,
            namespace=namespace,
            statuses=statuses,
            required=required,
            notes=notes,
        )

    def type_for_namespace(self, namespace: str) -> TypeDef | None:
        """Look up a TypeDef by namespace prefix."""
        types = self.parse()
        for typedef in types.values():
            if typedef.namespace == namespace:
                return typedef
        return None
```

### Step 4: Run tests to verify they pass

```bash
pytest tests/test_ontology.py -v
```

Expected: All 6 tests PASS.

### Step 5: Commit

```bash
git add src/living_graph/ontology.py tests/test_ontology.py
git commit -m "feat: add ontology parser for Convention/Ontology"
```

---

## Task 3: Entity Resolution

**Files:**
- Create: `src/living_graph/entity_resolution.py`
- Create: `tests/test_entity_resolution.py`

### Step 1: Write the failing test

```python
# ABOUTME: Tests for entity resolution — dedup-on-create and retroactive scanning.
# ABOUTME: Uses Test/ namespace pages to verify matching without polluting real data.

import time
from living_graph.entity_resolution import EntityResolver


def test_exact_match_finds_existing(roam, test_namespace):
    """Exact case-insensitive match on an existing page."""
    roam.create_page("Test/Entity Match")
    time.sleep(1)
    resolver = EntityResolver(roam)
    result = resolver.resolve("Test/", "Entity Match")
    assert result is not None
    assert result["title"] == "Test/Entity Match"
    assert "uid" in result


def test_case_insensitive_match(roam, test_namespace):
    """Should match regardless of case."""
    roam.create_page("Test/Case Test")
    time.sleep(1)
    resolver = EntityResolver(roam)
    result = resolver.resolve("Test/", "case test")
    assert result is not None
    assert result["title"] == "Test/Case Test"


def test_no_match_returns_none(roam, test_namespace):
    """Should return None when no match exists."""
    resolver = EntityResolver(roam)
    result = resolver.resolve("Test/", "Nonexistent Page XYZ123")
    assert result is None


def test_normalize_title(roam):
    """Title normalization: strip whitespace, title case."""
    resolver = EntityResolver(roam)
    assert resolver.normalize("  shane parrish  ") == "Shane Parrish"
    assert resolver.normalize("LOUD TITLE") == "Loud Title"
    assert resolver.normalize("already Good") == "Already Good"


def test_retroactive_scan_finds_duplicates(roam, test_namespace):
    """Retroactive scan should find near-duplicate titles in a namespace."""
    roam.create_page("Test/Duplicate One")
    roam.create_page("Test/duplicate one")
    time.sleep(1)
    resolver = EntityResolver(roam)
    dupes = resolver.scan_duplicates("Test/")
    # Should find the case-variant pair
    assert len(dupes) > 0
    titles = set()
    for group in dupes:
        for entry in group:
            titles.add(entry["title"])
    assert "Test/Duplicate One" in titles or "Test/duplicate one" in titles


def test_fuzzy_match(roam, test_namespace):
    """Fuzzy matching should find close but not exact matches."""
    roam.create_page("Test/Shane Parrish")
    time.sleep(1)
    resolver = EntityResolver(roam)
    matches = resolver.fuzzy_match("Test/", "Shaun Parish", threshold=0.7)
    assert len(matches) > 0
    assert any("Shane Parrish" in m["title"] for m in matches)


def test_resolve_or_create_existing(roam, test_namespace):
    """resolve_or_create should return existing page, not create new."""
    roam.create_page("Test/Existing Entity")
    time.sleep(1)
    resolver = EntityResolver(roam)
    result = resolver.resolve_or_create("Test/", "existing entity")
    assert result["title"] == "Test/Existing Entity"
    assert result["created"] is False


def test_resolve_or_create_new(roam, test_namespace):
    """resolve_or_create should create when no match exists."""
    resolver = EntityResolver(roam)
    result = resolver.resolve_or_create("Test/", "Brand New Entity")
    assert result["title"] == "Test/Brand New Entity"
    assert result["created"] is True
```

### Step 2: Run tests to verify they fail

```bash
pytest tests/test_entity_resolution.py -v
```

Expected: FAIL — `living_graph.entity_resolution` does not exist.

### Step 3: Write minimal implementation

```python
# ABOUTME: Entity resolution for Roam namespaced pages.
# ABOUTME: Case-insensitive dedup-on-create, title normalization, fuzzy matching, retroactive scanning.

from difflib import SequenceMatcher
from living_graph.client import RoamClient


class EntityResolver:
    """Resolve entities within Roam namespaces to prevent duplicates."""

    def __init__(self, client: RoamClient):
        self._client = client

    def normalize(self, title: str) -> str:
        """Normalize a title: strip whitespace, title case."""
        return title.strip().title()

    def resolve(self, namespace: str, title: str) -> dict | None:
        """Find an existing page matching namespace + title (case-insensitive).

        Returns {"uid": str, "title": str} or None.
        """
        # Query all pages in the namespace
        pages = self._client.q(
            "[:find ?uid ?title :where "
            "[?p :node/title ?title] "
            "[?p :block/uid ?uid] "
            f'[(clojure.string/starts-with? ?title "{namespace}")]]'
        )
        # Case-insensitive match on the name part (after namespace prefix)
        target = title.strip().lower()
        for uid, page_title in pages:
            name_part = page_title[len(namespace) :]
            if name_part.lower() == target:
                return {"uid": uid, "title": page_title}
        return None

    def fuzzy_match(
        self, namespace: str, title: str, threshold: float = 0.8
    ) -> list[dict]:
        """Find pages in namespace with similar titles (above threshold).

        Returns list of {"uid": str, "title": str, "score": float}.
        """
        pages = self._client.q(
            "[:find ?uid ?title :where "
            "[?p :node/title ?title] "
            "[?p :block/uid ?uid] "
            f'[(clojure.string/starts-with? ?title "{namespace}")]]'
        )
        target = title.strip().lower()
        matches = []
        for uid, page_title in pages:
            name_part = page_title[len(namespace) :].lower()
            score = SequenceMatcher(None, target, name_part).ratio()
            if score >= threshold:
                matches.append({"uid": uid, "title": page_title, "score": score})
        return sorted(matches, key=lambda m: m["score"], reverse=True)

    def scan_duplicates(self, namespace: str) -> list[list[dict]]:
        """Scan a namespace for groups of near-duplicate titles.

        Returns list of groups, where each group is a list of {"uid", "title"}.
        """
        pages = self._client.q(
            "[:find ?uid ?title :where "
            "[?p :node/title ?title] "
            "[?p :block/uid ?uid] "
            f'[(clojure.string/starts-with? ?title "{namespace}")]]'
        )
        # Group by lowercased name part
        groups: dict[str, list[dict]] = {}
        for uid, title in pages:
            key = title[len(namespace) :].strip().lower()
            groups.setdefault(key, []).append({"uid": uid, "title": title})

        # Return only groups with more than one entry (= duplicates)
        return [group for group in groups.values() if len(group) > 1]

    def resolve_or_create(self, namespace: str, title: str) -> dict:
        """Resolve an entity or create it if no match exists.

        Returns {"uid": str, "title": str, "created": bool}.
        """
        existing = self.resolve(namespace, title)
        if existing:
            return {**existing, "created": False}

        normalized = self.normalize(title)
        full_title = f"{namespace}{normalized}"
        self._client.create_page(full_title)

        # Fetch the UID of the newly created page
        results = self._client.q(
            f'[:find ?uid :where [?p :node/title "{full_title}"] [?p :block/uid ?uid]]'
        )
        uid = results[0][0] if results else ""
        return {"uid": uid, "title": full_title, "created": True}
```

### Step 4: Run tests to verify they pass

```bash
pytest tests/test_entity_resolution.py -v
```

Expected: All 8 tests PASS.

### Step 5: Commit

```bash
git add src/living_graph/entity_resolution.py tests/test_entity_resolution.py
git commit -m "feat: add entity resolution with dedup, fuzzy matching, retroactive scan"
```

---

## Task 4: Validation Scanner

**Files:**
- Create: `src/living_graph/validation.py`
- Create: `tests/test_validation.py`

### Step 1: Write the failing test

```python
# ABOUTME: Tests for the validation scanner.
# ABOUTME: Creates intentionally invalid Test/ pages and verifies the scanner catches issues.

import time
from living_graph.validation import ValidationScanner, Issue


def test_missing_required_attrs(roam, test_namespace):
    """Page missing required attributes should produce issues."""
    roam.create_page("Test/Validation Person")
    time.sleep(1)
    scanner = ValidationScanner(roam)
    issues = scanner.validate_page("Test/Validation Person")
    # Person requires Role:: and Related:: — both missing
    attr_issues = [i for i in issues if i.kind == "missing_attr"]
    assert len(attr_issues) >= 2
    attr_names = {i.detail for i in attr_issues}
    assert "Role" in attr_names
    assert "Related" in attr_names


def test_valid_page_no_issues(roam, test_namespace):
    """Page with all required attrs should produce no issues."""
    roam.create_page("Test/Validation Good Person")
    time.sleep(1)
    pages = roam.q(
        '[:find ?uid :where [?p :node/title "Test/Validation Good Person"] [?p :block/uid ?uid]]'
    )
    page_uid = pages[0][0]
    roam.create_block(page_uid, "Role:: Engineer", 0)
    roam.create_block(page_uid, "Related:: [[Test/Something]]", 1)
    time.sleep(1)
    scanner = ValidationScanner(roam)
    issues = scanner.validate_page("Test/Validation Good Person")
    attr_issues = [i for i in issues if i.kind == "missing_attr"]
    assert len(attr_issues) == 0


def test_invalid_status(roam, test_namespace):
    """Page with an invalid status value should produce an issue."""
    roam.create_page("Test/Validation Bad Project")
    time.sleep(1)
    pages = roam.q(
        '[:find ?uid :where [?p :node/title "Test/Validation Bad Project"] [?p :block/uid ?uid]]'
    )
    page_uid = pages[0][0]
    roam.create_block(page_uid, "Track:: Professional", 0)
    roam.create_block(page_uid, "Status:: banana", 1)
    roam.create_block(page_uid, "Related:: [[Test/Something]]", 2)
    time.sleep(1)
    scanner = ValidationScanner(roam)
    issues = scanner.validate_page("Test/Validation Bad Project")
    status_issues = [i for i in issues if i.kind == "invalid_status"]
    assert len(status_issues) == 1
    assert "banana" in status_issues[0].detail


def test_scan_namespace(roam, test_namespace):
    """Scanning a namespace returns issues grouped by page."""
    roam.create_page("Test/Scan Person A")
    roam.create_page("Test/Scan Person B")
    time.sleep(1)
    scanner = ValidationScanner(roam)
    # Override type detection so Test/ pages are treated as Person type
    report = scanner.scan_namespace("Test/Scan ", type_name="Person")
    assert len(report) == 2
    for title, issues in report.items():
        assert any(i.kind == "missing_attr" for i in issues)


def test_stub_detection(roam, test_namespace):
    """Page with no children should be flagged as a stub."""
    roam.create_page("Test/Stub Page")
    time.sleep(1)
    scanner = ValidationScanner(roam)
    issues = scanner.validate_page("Test/Stub Page")
    stub_issues = [i for i in issues if i.kind == "stub"]
    assert len(stub_issues) == 1


def test_issue_severity(roam, test_namespace):
    """Issues should have appropriate severity levels."""
    roam.create_page("Test/Severity Check")
    time.sleep(1)
    scanner = ValidationScanner(roam)
    issues = scanner.validate_page("Test/Severity Check")
    # Missing attrs are warnings, stubs are info
    for issue in issues:
        assert issue.severity in ("error", "warning", "info")
```

### Step 2: Run tests to verify they fail

```bash
pytest tests/test_validation.py -v
```

Expected: FAIL — `living_graph.validation` does not exist.

### Step 3: Write minimal implementation

```python
# ABOUTME: Validation scanner for Roam namespaced pages.
# ABOUTME: Checks pages against Convention/Ontology for missing attrs, invalid statuses, stubs.

import re
from dataclasses import dataclass
from living_graph.client import RoamClient
from living_graph.ontology import OntologyParser


@dataclass
class Issue:
    """A validation issue found on a page."""

    kind: str  # missing_attr, invalid_status, stub, orphan
    severity: str  # error, warning, info
    page_title: str
    detail: str


class ValidationScanner:
    """Scan namespaced pages against the ontology for issues."""

    def __init__(self, client: RoamClient):
        self._client = client
        self._parser = OntologyParser(client)

    def _detect_type(self, title: str) -> str | None:
        """Detect the type name from a page title's namespace prefix."""
        types = self._parser.parse()
        for typedef in types.values():
            if title.startswith(typedef.namespace):
                return typedef.name
        return None

    def _get_page_attrs(self, title: str) -> tuple[str | None, dict[str, str]]:
        """Fetch a page's UID and its attribute blocks."""
        results = self._client.q(
            f'[:find ?uid :where [?p :node/title "{title}"] [?p :block/uid ?uid]]'
        )
        if not results:
            return None, {}

        page_uid = results[0][0]
        tree = self._client.pull(
            "[:block/uid :block/string {:block/children [:block/string :block/uid]}]",
            f'[:block/uid "{page_uid}"]',
        )
        children = tree.get(":block/children", tree.get("block/children", []))
        attrs = {}
        for child in children:
            text = child.get(":block/string", child.get("block/string", ""))
            if "::" in text:
                key, val = text.split("::", 1)
                attrs[key.strip()] = val.strip()
        return page_uid, attrs

    def validate_page(
        self, title: str, type_name: str | None = None
    ) -> list[Issue]:
        """Validate a single page against its type definition."""
        issues = []
        if type_name is None:
            type_name = self._detect_type(title)
        if type_name is None:
            return issues  # Not a typed page

        types = self._parser.parse()
        typedef = types.get(type_name)
        if typedef is None:
            return issues

        page_uid, attrs = self._get_page_attrs(title)
        if page_uid is None:
            issues.append(
                Issue("orphan", "warning", title, "Page not found in graph")
            )
            return issues

        # Check required attributes
        for req in typedef.required:
            # Normalize: required might be "Role" but page has "Role::"
            req_clean = req.rstrip(":")
            if req_clean not in attrs:
                issues.append(
                    Issue("missing_attr", "warning", title, req_clean)
                )

        # Check status validity
        if typedef.statuses and "Status" in attrs:
            status_val = attrs["Status"].strip().lower()
            valid = [s.lower() for s in typedef.statuses]
            if status_val and status_val not in valid:
                issues.append(
                    Issue(
                        "invalid_status",
                        "error",
                        title,
                        f"'{attrs['Status'].strip()}' not in {typedef.statuses}",
                    )
                )

        # Check for stubs (no child blocks at all)
        tree = self._client.pull(
            "[:block/uid {:block/children [:block/uid]}]",
            f'[:block/uid "{page_uid}"]',
        )
        children = tree.get(":block/children", tree.get("block/children", []))
        if not children:
            issues.append(Issue("stub", "info", title, "Page has no content"))

        return issues

    def scan_namespace(
        self, namespace: str, type_name: str | None = None
    ) -> dict[str, list[Issue]]:
        """Scan all pages in a namespace and return issues by page title."""
        pages = self._client.q(
            "[:find ?title :where "
            "[?p :node/title ?title] "
            f'[(clojure.string/starts-with? ?title "{namespace}")]]'
        )
        report = {}
        for (title,) in pages:
            issues = self.validate_page(title, type_name=type_name)
            report[title] = issues
        return report

    def scan_all(self) -> dict[str, list[Issue]]:
        """Scan all typed namespaces and return full report."""
        types = self._parser.parse()
        report = {}
        for typedef in types.values():
            ns_report = self.scan_namespace(typedef.namespace)
            report.update(ns_report)
        return report
```

### Step 4: Run tests to verify they pass

```bash
pytest tests/test_validation.py -v
```

Expected: All 6 tests PASS.

### Step 5: Commit

```bash
git add src/living_graph/validation.py tests/test_validation.py
git commit -m "feat: add validation scanner for ontology compliance"
```

---

## Task 5: Interlinking Helpers

**Files:**
- Create: `src/living_graph/interlinking.py`
- Create: `tests/test_interlinking.py`

### Step 1: Write the failing test

```python
# ABOUTME: Tests for interlinking helpers.
# ABOUTME: Verifies bidirectional Related:: wiring between Test/ namespace pages.

import time
from living_graph.interlinking import Interlinker


def test_find_unlinked_references(roam, test_namespace):
    """Find pages that reference a page but aren't in its Related:: attr."""
    roam.create_page("Test/Link Target")
    roam.create_page("Test/Link Source")
    time.sleep(1)
    # Add a reference from source to target
    pages = roam.q(
        '[:find ?uid :where [?p :node/title "Test/Link Source"] [?p :block/uid ?uid]]'
    )
    source_uid = pages[0][0]
    roam.create_block(source_uid, "Mentions [[Test/Link Target]] in passing", 0)
    time.sleep(1)

    linker = Interlinker(roam)
    unlinked = linker.find_unlinked_references("Test/Link Target")
    assert any("Test/Link Source" in ref["title"] for ref in unlinked)


def test_add_related(roam, test_namespace):
    """Add Related:: attribute to a page."""
    roam.create_page("Test/Relate A")
    roam.create_page("Test/Relate B")
    time.sleep(1)
    pages_a = roam.q(
        '[:find ?uid :where [?p :node/title "Test/Relate A"] [?p :block/uid ?uid]]'
    )
    uid_a = pages_a[0][0]

    linker = Interlinker(roam)
    linker.add_related(uid_a, ["Test/Relate B"])
    time.sleep(1)

    # Verify the Related:: block was added
    tree = roam.pull(
        "[:block/uid {:block/children [:block/string]}]",
        f'[:block/uid "{uid_a}"]',
    )
    children = tree.get(":block/children", tree.get("block/children", []))
    strings = [
        c.get(":block/string", c.get("block/string", "")) for c in children
    ]
    assert any("Related::" in s and "Test/Relate B" in s for s in strings)


def test_add_related_appends_to_existing(roam, test_namespace):
    """If Related:: already exists, append rather than duplicate."""
    roam.create_page("Test/Append Related")
    time.sleep(1)
    pages = roam.q(
        '[:find ?uid :where [?p :node/title "Test/Append Related"] [?p :block/uid ?uid]]'
    )
    page_uid = pages[0][0]
    roam.create_block(page_uid, "Related:: [[Test/Existing Link]]", 0)
    time.sleep(1)

    linker = Interlinker(roam)
    linker.add_related(page_uid, ["Test/New Link"])
    time.sleep(1)

    tree = roam.pull(
        "[:block/uid {:block/children [:block/string]}]",
        f'[:block/uid "{page_uid}"]',
    )
    children = tree.get(":block/children", tree.get("block/children", []))
    strings = [
        c.get(":block/string", c.get("block/string", "")) for c in children
    ]
    related = [s for s in strings if s.startswith("Related::")]
    assert len(related) == 1  # Should NOT create a second Related:: block
    assert "Test/Existing Link" in related[0]
    assert "Test/New Link" in related[0]


def test_bidirectional_link(roam, test_namespace):
    """Linking A→B should also link B→A."""
    roam.create_page("Test/Bi A")
    roam.create_page("Test/Bi B")
    time.sleep(1)
    pages_a = roam.q(
        '[:find ?uid :where [?p :node/title "Test/Bi A"] [?p :block/uid ?uid]]'
    )
    pages_b = roam.q(
        '[:find ?uid :where [?p :node/title "Test/Bi B"] [?p :block/uid ?uid]]'
    )
    uid_a = pages_a[0][0]
    uid_b = pages_b[0][0]

    linker = Interlinker(roam)
    linker.link_bidirectional(uid_a, "Test/Bi A", uid_b, "Test/Bi B")
    time.sleep(1)

    # Check A has Related:: to B
    tree_a = roam.pull(
        "[:block/uid {:block/children [:block/string]}]",
        f'[:block/uid "{uid_a}"]',
    )
    strings_a = [
        c.get(":block/string", c.get("block/string", ""))
        for c in tree_a.get(":block/children", tree_a.get("block/children", []))
    ]
    assert any("Test/Bi B" in s for s in strings_a)

    # Check B has Related:: to A
    tree_b = roam.pull(
        "[:block/uid {:block/children [:block/string]}]",
        f'[:block/uid "{uid_b}"]',
    )
    strings_b = [
        c.get(":block/string", c.get("block/string", ""))
        for c in tree_b.get(":block/children", tree_b.get("block/children", []))
    ]
    assert any("Test/Bi A" in s for s in strings_b)
```

### Step 2: Run tests to verify they fail

```bash
pytest tests/test_interlinking.py -v
```

Expected: FAIL — `living_graph.interlinking` does not exist.

### Step 3: Write minimal implementation

```python
# ABOUTME: Interlinking helpers for bidirectional Related:: wiring.
# ABOUTME: Finds unlinked references and adds/appends Related:: attributes between pages.

import re
from living_graph.client import RoamClient


class Interlinker:
    """Wire bidirectional Related:: attributes between Roam pages."""

    def __init__(self, client: RoamClient):
        self._client = client

    def find_unlinked_references(self, page_title: str) -> list[dict]:
        """Find pages that reference page_title but aren't in its Related:: attr.

        Returns list of {"uid": str, "title": str}.
        """
        # Find all blocks that reference this page
        refs = self._client.q(
            "[:find ?page-uid ?page-title "
            ":where "
            "[?ref-page :node/title ?ref-title] "
            f'[(= ?ref-title "{page_title}")] '
            "[?b :block/refs ?ref-page] "
            "[?b :block/page ?source-page] "
            "[?source-page :block/uid ?page-uid] "
            "[?source-page :node/title ?page-title]]"
        )

        # Get current Related:: targets for the page
        current_related = self._get_related_titles(page_title)

        # Filter out self-references and already-linked pages
        unlinked = []
        seen = set()
        for uid, title in refs:
            if title == page_title or title in current_related or title in seen:
                continue
            seen.add(title)
            unlinked.append({"uid": uid, "title": title})
        return unlinked

    def _get_related_titles(self, page_title: str) -> set[str]:
        """Extract page titles from a page's Related:: attribute."""
        results = self._client.q(
            f'[:find ?uid :where [?p :node/title "{page_title}"] [?p :block/uid ?uid]]'
        )
        if not results:
            return set()

        page_uid = results[0][0]
        tree = self._client.pull(
            "[:block/uid {:block/children [:block/string]}]",
            f'[:block/uid "{page_uid}"]',
        )
        children = tree.get(":block/children", tree.get("block/children", []))
        for child in children:
            text = child.get(":block/string", child.get("block/string", ""))
            if text.startswith("Related::"):
                return set(re.findall(r"\[\[([^\]]+)\]\]", text))
        return set()

    def _find_related_block(self, page_uid: str) -> tuple[str | None, str]:
        """Find the Related:: block on a page. Returns (block_uid, current_text)."""
        tree = self._client.pull(
            "[:block/uid {:block/children [:block/string :block/uid]}]",
            f'[:block/uid "{page_uid}"]',
        )
        children = tree.get(":block/children", tree.get("block/children", []))
        for child in children:
            text = child.get(":block/string", child.get("block/string", ""))
            uid = child.get(":block/uid", child.get("block/uid", ""))
            if text.startswith("Related::"):
                return uid, text
        return None, ""

    def add_related(self, page_uid: str, titles: list[str]) -> None:
        """Add titles to a page's Related:: attribute. Appends if exists, creates if not."""
        block_uid, current_text = self._find_related_block(page_uid)
        links = " ".join(f"[[{t}]]" for t in titles)

        if block_uid:
            # Append to existing
            new_text = f"{current_text} {links}"
            self._client.update_block(block_uid, new_text)
        else:
            # Create new Related:: block
            self._client.create_block(page_uid, f"Related:: {links}", "last")

    def link_bidirectional(
        self,
        uid_a: str,
        title_a: str,
        uid_b: str,
        title_b: str,
    ) -> None:
        """Create bidirectional Related:: links between two pages."""
        self.add_related(uid_a, [title_b])
        self.add_related(uid_b, [title_a])
```

### Step 4: Run tests to verify they pass

```bash
pytest tests/test_interlinking.py -v
```

Expected: All 4 tests PASS.

### Step 5: Commit

```bash
git add src/living_graph/interlinking.py tests/test_interlinking.py
git commit -m "feat: add interlinking helpers for bidirectional Related:: wiring"
```

---

## Task 6: Mutation Logging

**Files:**
- Create: `src/living_graph/mutation_log.py`
- Create: `tests/test_mutation_log.py`

### Step 1: Write the failing test

```python
# ABOUTME: Tests for mutation logging.
# ABOUTME: Verifies Run/ pages are created with structured mutation entries.

import time
from living_graph.mutation_log import MutationLogger


def test_create_run(roam, test_namespace):
    """Creating a run should produce a Run/ page."""
    # Use Test/ namespace to avoid polluting real Run/ pages
    logger = MutationLogger(roam, namespace_prefix="Test/Run/")
    run = logger.create_run("TestWorker", "2026-02-24")
    assert run["title"] == "Test/Run/TestWorker 2026-02-24"
    assert "uid" in run


def test_log_mutation(roam, test_namespace):
    """Logging a mutation should create a child block on the Run page."""
    logger = MutationLogger(roam, namespace_prefix="Test/Run/")
    run = logger.create_run("TestWorker", "2026-02-24-log")
    logger.log(
        run_uid=run["uid"],
        action="create",
        target="Test/Some Page",
        changes={"title": "Test/Some Page", "attrs": ["Role::", "Related::"]},
    )
    time.sleep(1)

    tree = roam.pull(
        "[:block/uid {:block/children [:block/string]}]",
        f'[:block/uid "{run["uid"]}"]',
    )
    children = tree.get(":block/children", tree.get("block/children", []))
    strings = [
        c.get(":block/string", c.get("block/string", "")) for c in children
    ]
    # Should have at least the metadata blocks + the mutation entry
    mutation_blocks = [s for s in strings if "create" in s.lower()]
    assert len(mutation_blocks) >= 1


def test_close_run(roam, test_namespace):
    """Closing a run should update its Status and Summary attributes."""
    logger = MutationLogger(roam, namespace_prefix="Test/Run/")
    run = logger.create_run("TestWorker", "2026-02-24-close")
    logger.close_run(run["uid"], status="completed", summary="3 pages created")
    time.sleep(1)

    tree = roam.pull(
        "[:block/uid {:block/children [:block/string]}]",
        f'[:block/uid "{run["uid"]}"]',
    )
    children = tree.get(":block/children", tree.get("block/children", []))
    strings = [
        c.get(":block/string", c.get("block/string", "")) for c in children
    ]
    assert any("Status:: completed" in s for s in strings)
    assert any("3 pages created" in s for s in strings)


def test_multiple_mutations_in_run(roam, test_namespace):
    """Multiple mutations should appear as ordered children."""
    logger = MutationLogger(roam, namespace_prefix="Test/Run/")
    run = logger.create_run("TestWorker", "2026-02-24-multi")
    logger.log(run["uid"], "create", "Test/Page A", {"title": "Test/Page A"})
    logger.log(run["uid"], "edit", "Test/Page B", {"attr": "Status", "old": "active", "new": "paused"})
    logger.log(run["uid"], "delete", "Test/Page C", {})
    time.sleep(1)

    tree = roam.pull(
        "[:block/uid {:block/children [:block/string]}]",
        f'[:block/uid "{run["uid"]}"]',
    )
    children = tree.get(":block/children", tree.get("block/children", []))
    strings = [
        c.get(":block/string", c.get("block/string", "")) for c in children
    ]
    # Should have metadata blocks + 3 mutation entries
    mutation_blocks = [s for s in strings if any(
        op in s.lower() for op in ["create", "edit", "delete"]
    )]
    assert len(mutation_blocks) >= 3
```

### Step 2: Run tests to verify they fail

```bash
pytest tests/test_mutation_log.py -v
```

Expected: FAIL — `living_graph.mutation_log` does not exist.

### Step 3: Write minimal implementation

```python
# ABOUTME: Mutation logging for worker operations.
# ABOUTME: Creates Run/ pages with structured child blocks tracking every graph mutation.

import json
from datetime import datetime, timezone
from living_graph.client import RoamClient


class MutationLogger:
    """Log worker mutations as structured Run/ pages in Roam."""

    def __init__(self, client: RoamClient, namespace_prefix: str = "Run/"):
        self._client = client
        self._prefix = namespace_prefix

    def create_run(self, worker: str, date: str) -> dict:
        """Create a new Run/ page for a worker execution.

        Returns {"uid": str, "title": str}.
        """
        title = f"{self._prefix}{worker} {date}"
        self._client.create_page(title)

        results = self._client.q(
            f'[:find ?uid :where [?p :node/title "{title}"] [?p :block/uid ?uid]]'
        )
        uid = results[0][0] if results else ""

        # Add metadata blocks
        self._client.batch([
            {
                "action": "create-block",
                "location": {"parent-uid": uid, "order": 0},
                "block": {"string": f"Process:: [[{self._prefix.rstrip('/')}/{worker}]]"},
            },
            {
                "action": "create-block",
                "location": {"parent-uid": uid, "order": 1},
                "block": {"string": f"Date:: [[{self._format_date(date)}]]"},
            },
            {
                "action": "create-block",
                "location": {"parent-uid": uid, "order": 2},
                "block": {"string": "Status:: running"},
            },
        ])
        return {"uid": uid, "title": title}

    def log(
        self,
        run_uid: str,
        action: str,
        target: str,
        changes: dict,
    ) -> None:
        """Log a single mutation to a Run page."""
        timestamp = datetime.now(timezone.utc).strftime("%H:%M:%S")
        changes_str = json.dumps(changes, ensure_ascii=False)
        entry = f"`{timestamp}` **{action}** [[{target}]] `{changes_str}`"
        self._client.create_block(run_uid, entry, "last")

    def close_run(
        self, run_uid: str, status: str, summary: str
    ) -> None:
        """Close a run by updating Status and adding Summary."""
        # Find and update the Status:: block
        tree = self._client.pull(
            "[:block/uid {:block/children [:block/string :block/uid]}]",
            f'[:block/uid "{run_uid}"]',
        )
        children = tree.get(":block/children", tree.get("block/children", []))
        for child in children:
            text = child.get(":block/string", child.get("block/string", ""))
            uid = child.get(":block/uid", child.get("block/uid", ""))
            if text.startswith("Status::"):
                self._client.update_block(uid, f"Status:: {status}")
                break

        self._client.create_block(run_uid, f"Summary:: {summary}", "last")

    def _format_date(self, date_str: str) -> str:
        """Convert YYYY-MM-DD to Roam ordinal date format."""
        try:
            dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
            day = dt.day
            suffix = (
                "th" if 11 <= day <= 13 else {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")
            )
            return dt.strftime(f"%B {day}{suffix}, %Y")
        except ValueError:
            return date_str
```

### Step 4: Run tests to verify they pass

```bash
pytest tests/test_mutation_log.py -v
```

Expected: All 4 tests PASS.

### Step 5: Commit

```bash
git add src/living_graph/mutation_log.py tests/test_mutation_log.py
git commit -m "feat: add mutation logging with Run/ pages"
```

---

## Task 7: Integration Test — Full Workflow

**Files:**
- Create: `tests/test_integration.py`

One end-to-end test that exercises all modules together: resolve an entity, validate it, interlink it, log the mutations.

### Step 1: Write the test

```python
# ABOUTME: End-to-end integration test exercising all deterministic layer modules.
# ABOUTME: Simulates a curator-like workflow: resolve, validate, interlink, log.

import time
from living_graph.entity_resolution import EntityResolver
from living_graph.validation import ValidationScanner
from living_graph.interlinking import Interlinker
from living_graph.mutation_log import MutationLogger


def test_full_curator_workflow(roam, test_namespace):
    """Simulate a curator workflow across all modules."""
    # 1. Start a run
    logger = MutationLogger(roam, namespace_prefix="Test/Run/")
    run = logger.create_run("IntegrationTest", "2026-02-24-e2e")

    # 2. Resolve an entity (should create since it doesn't exist)
    resolver = EntityResolver(roam)
    entity = resolver.resolve_or_create("Test/", "Integration Person")
    assert entity["created"] is True
    logger.log(run["uid"], "create", entity["title"], {"created": True})

    time.sleep(1)

    # 3. Add some attributes so validation passes
    roam.create_block(entity["uid"], "Role:: Test Subject", 0)
    roam.create_block(entity["uid"], "Related:: [[Test/Integration Project]]", 1)

    # 4. Create a second entity to interlink with
    project = resolver.resolve_or_create("Test/", "Integration Project")
    logger.log(run["uid"], "create", project["title"], {"created": True})

    time.sleep(1)

    # 5. Validate the person page — should be clean now
    scanner = ValidationScanner(roam)
    issues = scanner.validate_page("Test/Integration Person", type_name="Person")
    attr_issues = [i for i in issues if i.kind == "missing_attr"]
    assert len(attr_issues) == 0

    # 6. Interlink the two pages
    linker = Interlinker(roam)
    linker.link_bidirectional(
        entity["uid"], entity["title"],
        project["uid"], project["title"],
    )
    logger.log(
        run["uid"],
        "edit",
        entity["title"],
        {"added_related": project["title"]},
    )

    # 7. Close the run
    logger.close_run(run["uid"], "completed", "2 entities, 1 interlink")

    time.sleep(1)

    # 8. Verify the run page has all entries
    tree = roam.pull(
        "[:block/uid {:block/children [:block/string]}]",
        f'[:block/uid "{run["uid"]}"]',
    )
    children = tree.get(":block/children", tree.get("block/children", []))
    strings = [
        c.get(":block/string", c.get("block/string", "")) for c in children
    ]
    assert any("Status:: completed" in s for s in strings)
    assert any("2 entities" in s for s in strings)
```

### Step 2: Run all tests

```bash
pytest tests/ -v
```

Expected: All tests PASS.

### Step 3: Commit

```bash
git add tests/test_integration.py
git commit -m "test: add end-to-end integration test for full workflow"
```

---

## Summary

| Task | Module | Tests | What it does |
|------|--------|-------|-------------|
| 0 | project setup | conftest | Repo, venv, fixtures, cleanup |
| 1 | client.py | 6 | Roam API wrapper with rate limiting |
| 2 | ontology.py | 6 | Parse Convention/Ontology into TypeDefs |
| 3 | entity_resolution.py | 8 | Case-insensitive dedup, fuzzy match, retroactive scan |
| 4 | validation.py | 6 | Check pages against ontology for compliance |
| 5 | interlinking.py | 4 | Bidirectional Related:: wiring |
| 6 | mutation_log.py | 4 | Run/ pages with structured mutation entries |
| 7 | integration test | 1 | End-to-end workflow across all modules |

**Total: 35 tests across 7 tasks.**

After this phase, the deterministic engine exists as a standalone Python library. Phase 3 (Curator) will import these modules and wire them into an LLM+deterministic pipeline.
