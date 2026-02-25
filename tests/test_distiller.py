# ABOUTME: Tests for the distiller pipeline.
# ABOUTME: Verifies epistemic knowledge extraction and page creation through all 4 stages.

import time
import pytest
from living_graph.distiller import DistillerPipeline


def test_distill_page_returns_result_dict(roam, claude):
    """Pipeline should return a structured result dict."""
    roam.create_page("Test/Distill Input")
    time.sleep(2)
    pages = roam.q(
        '[:find ?uid :where [?p :node/title "Test/Distill Input"] [?p :block/uid ?uid]]'
    )
    page_uid = pages[0][0]
    roam.create_block(
        page_uid,
        "Decided to skip writing tests for the utility module because it felt too simple to break.",
        0,
    )
    time.sleep(2)

    pipeline = DistillerPipeline(roam, claude, run_prefix="Test/Run/")
    result = pipeline.distill_page("Test/Distill Input")

    assert "blocks_processed" in result
    assert "insights_extracted" in result
    assert "pages_created" in result
    assert "run_uid" in result
    assert result["blocks_processed"] >= 1


def test_distill_page_empty_page(roam, claude):
    """Pipeline should handle pages with no blocks gracefully."""
    roam.create_page("Test/Distill Empty")
    time.sleep(2)

    pipeline = DistillerPipeline(roam, claude, run_prefix="Test/Run/")
    result = pipeline.distill_page("Test/Distill Empty")

    assert result["blocks_processed"] == 0
    assert result["insights_extracted"] == 0
    assert result["pages_created"] == 0


def test_distill_page_nonexistent(roam, claude):
    """Pipeline should handle nonexistent pages gracefully."""
    pipeline = DistillerPipeline(roam, claude, run_prefix="Test/Run/")
    result = pipeline.distill_page("Test/Nonexistent Page 12345")

    assert result["blocks_processed"] == 0


def test_build_epistemic_context(roam):
    """Should build a context string of existing epistemic pages."""
    pipeline = DistillerPipeline(roam, claude=None, run_prefix="Test/Run/")
    context = pipeline.build_epistemic_context()
    assert isinstance(context, str)
    # May or may not have existing epistemic pages — just verify it runs


def test_build_epistemic_ontology_summary(roam):
    """Should build an ontology summary limited to epistemic types."""
    pipeline = DistillerPipeline(roam, claude=None, run_prefix="Test/Run/")
    summary = pipeline.build_epistemic_ontology_summary()
    assert "Assumption" in summary
    assert "Decision" in summary
    assert "Constraint" in summary
    assert "Contradiction" in summary
    assert "Synthesis" in summary
    # Should NOT include non-epistemic types
    assert "Person" not in summary
    assert "Project" not in summary


def test_scope_enforcement(roam):
    """Distiller scope should reject non-epistemic creates."""
    pipeline = DistillerPipeline(roam, claude=None, run_prefix="Test/Run/")
    from living_graph.scope import ScopeError
    with pytest.raises(ScopeError):
        pipeline._scope.check("create", "Person/")
    with pytest.raises(ScopeError):
        pipeline._scope.check("edit", "Assumption/")
    with pytest.raises(ScopeError):
        pipeline._scope.check("delete", "Assumption/")
    # These should pass
    pipeline._scope.check("create", "Assumption/")
    pipeline._scope.check("create", "Decision/")
    pipeline._scope.check("read", "Person/")
