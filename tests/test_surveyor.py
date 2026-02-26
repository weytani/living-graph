# ABOUTME: Tests for the surveyor pipeline — 4-stage orchestrator.
# ABOUTME: Verifies embed/cluster/label/write stages, scope enforcement, and Run/ page creation.

import json
import os
import time

import pytest

from living_graph.surveyor import SurveyorPipeline


@pytest.fixture
def test_pages(roam):
    """Create a set of related test pages for surveying."""
    pages = [
        ("Test/Survey Page A", "A page about Python programming and testing"),
        ("Test/Survey Page B", "A page about Python unit tests and pytest"),
        ("Test/Survey Page C", "A page about cooking recipes and Italian food"),
    ]
    for title, content in pages:
        roam.create_page(title)
        time.sleep(2)
        results = roam.q(
            f'[:find ?uid :where [?p :node/title "{title}"] [?p :block/uid ?uid]]'
        )
        page_uid = results[0][0]
        roam.create_block(page_uid, content, 0)
        time.sleep(1)
    return [p[0] for p in pages]


def test_survey_returns_result_dict(roam, claude, test_pages, tmp_path):
    """Pipeline should return a structured result dict with expected keys."""
    pipeline = SurveyorPipeline(
        roam,
        claude,
        data_dir=str(tmp_path),
        run_prefix="Test/Run/",
    )
    result = pipeline.survey(page_titles=test_pages)

    assert "pages_embedded" in result
    assert "clusters_found" in result
    assert "relationships_written" in result
    assert "tags_written" in result
    assert "run_uid" in result
    assert result["pages_embedded"] == len(test_pages)


def test_survey_empty_input(roam, claude, tmp_path):
    """Pipeline should handle empty page list gracefully."""
    pipeline = SurveyorPipeline(
        roam,
        claude,
        data_dir=str(tmp_path),
        run_prefix="Test/Run/",
    )
    result = pipeline.survey(page_titles=[])

    assert result["pages_embedded"] == 0
    assert result["clusters_found"] == 0


def test_survey_creates_run_page(roam, claude, test_pages, tmp_path):
    """Pipeline should create a Run/ page with Process:: and Status:: blocks."""
    pipeline = SurveyorPipeline(
        roam,
        claude,
        data_dir=str(tmp_path),
        run_prefix="Test/Run/",
    )
    result = pipeline.survey(page_titles=test_pages)

    # Verify Run page exists and has metadata blocks
    run_uid = result["run_uid"]
    time.sleep(2)
    tree = roam.pull(
        "[:block/uid :block/string {:block/children [:block/string]}]",
        f'[:block/uid "{run_uid}"]',
    )
    children = tree.get(":block/children", [])
    child_texts = [c.get(":block/string", "") for c in children]

    assert any("Process::" in t for t in child_texts)
    assert any("Status::" in t for t in child_texts)


def test_survey_persists_state(roam, claude, test_pages, tmp_path):
    """Pipeline should persist page hashes and last_run to state file."""
    pipeline = SurveyorPipeline(
        roam,
        claude,
        data_dir=str(tmp_path),
        run_prefix="Test/Run/",
    )
    pipeline.survey(page_titles=test_pages)

    state_path = os.path.join(str(tmp_path), "surveyor_state.json")
    assert os.path.exists(state_path)
    with open(state_path) as f:
        state = json.load(f)
    assert "page_hashes" in state
    assert "last_run" in state
    assert state["last_run"] is not None


def test_scope_enforcement(roam, claude, tmp_path):
    """Surveyor must not create or delete pages."""
    from living_graph.scope import ScopeEnforcer, ScopeError

    enforcer = ScopeEnforcer("surveyor")

    # These should raise
    with pytest.raises(ScopeError):
        enforcer.check("create")
    with pytest.raises(ScopeError):
        enforcer.check("delete")

    # These should pass
    enforcer.check("read")
    enforcer.check("edit_tags")
    enforcer.check("edit_relationships")
