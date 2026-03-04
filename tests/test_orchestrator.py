# ABOUTME: Tests for the orchestrator module — sequential worker runner with Run/ logging.
# ABOUTME: Uses Test/Run/ namespace prefix to avoid polluting production Run/ pages.

import time

from living_graph.orchestrator import Orchestrator


class TestOrchestratorRunRecord:
    """Test that orchestrator creates and closes Run/ records correctly."""

    def test_creates_run_record(self, roam, claude):
        """Orchestrator creates a Run/Orchestrator page with worker results."""
        orch = Orchestrator(roam, claude, run_prefix="Test/Run/")
        # Run with a page unlikely to have content — minimizes API calls
        result = orch.run(target_date="2020-01-01")

        assert result["run_title"].startswith("Test/Run/Orchestrator")
        assert "workers" in result
        assert len(result["workers"]) == 4
        assert result["workers"][0]["name"] == "Curator"
        assert result["workers"][1]["name"] == "Distiller"
        assert result["workers"][2]["name"] == "Janitor"
        assert result["workers"][3]["name"] == "Surveyor"

        # Verify Run/ page exists in Roam with completed status
        time.sleep(2)
        run_uid = result["run_title"]
        # Find page UID from title
        uid_rows = roam.q(
            '[:find ?uid :where '
            '[?p :node/title ?title] '
            '[?p :block/uid ?uid] '
            '[(= ?title "' + run_uid + '")]]'
        )
        assert uid_rows, f"Run page not found: {run_uid}"
        page_uid = uid_rows[0][0]
        tree = roam.pull(
            "[:block/uid {:block/children [:block/string]}]",
            f'[:block/uid "{page_uid}"]',
        )
        children = tree.get(":block/children", [])
        texts = [c.get(":block/string", "") for c in children]
        assert any("Status:: completed" in t for t in texts)
        assert any("Summary::" in t for t in texts)

    def test_abort_on_worker_failure(self, roam, claude):
        """Orchestrator aborts and records failure when a worker raises."""
        orch = Orchestrator(roam, claude, run_prefix="Test/Run/")
        # Pass workers list with a bad worker name to trigger failure
        result = orch.run(
            target_date="2020-01-01",
            workers=["Curator", "BOGUS"],
        )

        assert result["status"] == "failed"
        assert result["failed_worker"] == "BOGUS"
        # Should still have completed Curator results
        assert result["workers"][0]["name"] == "Curator"
        assert result["workers"][0]["status"] == "completed"

        # Verify Run/ page shows failed status
        time.sleep(2)
        run_uid = result["run_title"]
        # Find page UID from title
        uid_rows = roam.q(
            '[:find ?uid :where '
            '[?p :node/title ?title] '
            '[?p :block/uid ?uid] '
            '[(= ?title "' + run_uid + '")]]'
        )
        assert uid_rows, f"Run page not found: {run_uid}"
        page_uid = uid_rows[0][0]
        tree = roam.pull(
            "[:block/uid {:block/children [:block/string]}]",
            f'[:block/uid "{page_uid}"]',
        )
        children = tree.get(":block/children", [])
        texts = [c.get(":block/string", "") for c in children]
        assert any("Status:: failed" in t for t in texts)
