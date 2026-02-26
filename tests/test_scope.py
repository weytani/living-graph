# ABOUTME: Tests for scope enforcement.
# ABOUTME: Verifies the hard permission matrix blocks disallowed operations.

import pytest
from living_graph.scope import ScopeEnforcer, ScopeError


def test_curator_can_create():
    scope = ScopeEnforcer("curator")
    scope.check("create", "Person/")  # Should not raise


def test_curator_can_edit():
    scope = ScopeEnforcer("curator")
    scope.check("edit", "Person/")  # Should not raise


def test_curator_cannot_delete():
    scope = ScopeEnforcer("curator")
    with pytest.raises(ScopeError, match="delete"):
        scope.check("delete", "Person/")


def test_janitor_cannot_create():
    scope = ScopeEnforcer("janitor")
    with pytest.raises(ScopeError, match="create"):
        scope.check("create", "Person/")


def test_janitor_can_delete():
    scope = ScopeEnforcer("janitor")
    scope.check("delete", "Person/")  # Should not raise


def test_distiller_can_only_create_epistemic():
    scope = ScopeEnforcer("distiller")
    scope.check("create", "Assumption/")  # Should not raise
    scope.check("create", "Decision/")  # Should not raise
    with pytest.raises(ScopeError, match="create"):
        scope.check("create", "Person/")


def test_surveyor_can_only_edit_tags():
    scope = ScopeEnforcer("surveyor")
    scope.check("edit_tags", "Person/")  # Should not raise
    with pytest.raises(ScopeError, match="create"):
        scope.check("create", "Person/")
    with pytest.raises(ScopeError, match="delete"):
        scope.check("delete", "Person/")


def test_surveyor_can_edit_relationships():
    """Surveyor should be allowed to add/edit relationship attributes."""
    enforcer = ScopeEnforcer("surveyor")
    enforcer.check("edit_relationships")  # Should not raise


def test_surveyor_cannot_create():
    """Surveyor cannot create pages."""
    enforcer = ScopeEnforcer("surveyor")
    with pytest.raises(ScopeError):
        enforcer.check("create")


def test_surveyor_cannot_edit():
    """Surveyor cannot edit page content (only tags and relationships)."""
    enforcer = ScopeEnforcer("surveyor")
    with pytest.raises(ScopeError):
        enforcer.check("edit")


def test_surveyor_cannot_delete():
    """Surveyor cannot delete anything."""
    enforcer = ScopeEnforcer("surveyor")
    with pytest.raises(ScopeError):
        enforcer.check("delete")


def test_unknown_worker_raises():
    with pytest.raises(ScopeError, match="Unknown worker"):
        ScopeEnforcer("unknown")
