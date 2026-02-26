# ABOUTME: Scope enforcement for living graph workers.
# ABOUTME: Hard permission matrix — violations raise ScopeError, not warnings.

from __future__ import annotations

EPISTEMIC_NAMESPACES = {
    "Assumption/", "Constraint/", "Contradiction/", "Synthesis/", "Decision/"
}

# Permission matrix: {worker: {operation: True | False | "epistemic_only"}}
PERMISSIONS = {
    "curator": {
        "read": True,
        "create": True,
        "edit": True,
        "delete": False,
    },
    "janitor": {
        "read": True,
        "create": False,
        "edit": True,
        "delete": True,
    },
    "distiller": {
        "read": True,
        "create": "epistemic_only",
        "edit": False,
        "delete": False,
    },
    "surveyor": {
        "read": True,
        "create": False,
        "edit": False,
        "edit_tags": True,
        "edit_relationships": True,
        "delete": False,
    },
}


class ScopeError(Exception):
    """Raised when a worker attempts an operation outside its scope."""


class ScopeEnforcer:
    """Enforces the hard permission matrix for a specific worker."""

    def __init__(self, worker: str):
        if worker not in PERMISSIONS:
            raise ScopeError(f"Unknown worker: {worker}")
        self.worker = worker
        self._perms = PERMISSIONS[worker]

    def check(self, operation: str, namespace: str = "") -> None:
        """Check if an operation is allowed. Raises ScopeError if not."""
        perm = self._perms.get(operation)

        if perm is True:
            return
        if perm is False or perm is None:
            raise ScopeError(
                f"Worker '{self.worker}' cannot {operation} "
                f"(namespace: {namespace})"
            )
        if perm == "epistemic_only":
            if namespace in EPISTEMIC_NAMESPACES:
                return
            raise ScopeError(
                f"Worker '{self.worker}' can only {operation} epistemic types, "
                f"not {namespace}"
            )
