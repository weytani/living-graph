# ABOUTME: Sequential worker orchestrator with abort-on-failure.
# ABOUTME: Runs curator -> distiller -> janitor -> surveyor, writes a Run/ record to Roam.

import time
from datetime import date, timedelta

from living_graph.mutation_log import MutationLogger


DEFAULT_WORKERS = ["Curator", "Distiller", "Janitor", "Surveyor"]
WORKER_PAUSE_SECONDS = 10


def _ordinal_date(d: date) -> str:
    """Convert a date to Roam ordinal format: 'February 24th, 2026'."""
    day = d.day
    if 11 <= day <= 13:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")
    return d.strftime(f"%B {day}{suffix}, %Y")


class Orchestrator:
    """Run all living-graph workers sequentially with Run/ logging."""

    def __init__(self, roam, claude, run_prefix="Run/", data_dir="data"):
        self._roam = roam
        self._claude = claude
        self._logger = MutationLogger(roam, namespace_prefix=run_prefix)
        self._data_dir = data_dir

    def run(self, target_date=None, catch_up=0, workers=None):
        """Execute workers in sequence. Abort on first failure.

        Args:
            target_date: ISO date string (default: today)
            catch_up: Also process previous N days for date-driven workers
            workers: List of worker names (default: all 4)

        Returns:
            dict with run_title, status, workers list, and failed_worker if any
        """
        worker_names = workers or DEFAULT_WORKERS
        today_iso = target_date or date.today().isoformat()
        target = date.fromisoformat(today_iso)

        # Build page list for date-driven workers
        pages = [_ordinal_date(target)]
        for i in range(1, catch_up + 1):
            pages.append(_ordinal_date(target - timedelta(days=i)))

        # Create orchestrator Run/ record
        run = self._logger.create_run("Orchestrator", today_iso)
        self._logger.log(run["uid"], "start", "orchestrator", {
            "workers": worker_names,
            "pages": pages,
        })

        results = []
        failed_worker = None

        for i, name in enumerate(worker_names):
            if i > 0:
                time.sleep(WORKER_PAUSE_SECONDS)

            self._logger.log(run["uid"], "start_worker", name, {})

            try:
                worker_result = self._run_worker(name, pages)
                results.append({"name": name, "status": "completed", "result": worker_result})
                self._logger.log(run["uid"], "complete_worker", name, worker_result)
            except Exception as exc:
                results.append({"name": name, "status": "failed", "error": str(exc)})
                self._logger.log(run["uid"], "worker_failed", name, {"error": str(exc)})
                failed_worker = name
                break

        status = "failed" if failed_worker else "completed"
        summary_parts = [f"{r['name']}: {r['status']}" for r in results]
        summary = "; ".join(summary_parts)
        self._logger.close_run(run["uid"], status=status, summary=summary)

        return {
            "run_title": run["title"],
            "status": status,
            "workers": results,
            "failed_worker": failed_worker,
        }

    def _run_worker(self, name, pages):
        """Dispatch to the appropriate worker pipeline."""
        if name == "Curator":
            return self._run_curator(pages)
        elif name == "Distiller":
            return self._run_distiller(pages)
        elif name == "Janitor":
            return self._run_janitor()
        elif name == "Surveyor":
            return self._run_surveyor()
        else:
            raise ValueError(f"Unknown worker: {name}")

    def _run_curator(self, pages):
        from living_graph.curator import CuratorPipeline
        pipeline = CuratorPipeline(self._roam, self._claude)
        totals = {"blocks_processed": 0, "entities_resolved": 0, "entities_enriched": 0}
        for page_title in pages:
            result = pipeline.curate_page(page_title)
            for key in totals:
                totals[key] += result[key]
        return totals

    def _run_distiller(self, pages):
        from living_graph.distiller import DistillerPipeline
        pipeline = DistillerPipeline(self._roam, self._claude)
        totals = {"blocks_processed": 0, "insights_extracted": 0, "pages_created": 0, "pages_resolved": 0}
        for page_title in pages:
            result = pipeline.distill_page(page_title)
            for key in totals:
                totals[key] += result[key]
        return totals

    def _run_janitor(self):
        from living_graph.janitor import JanitorPipeline
        pipeline = JanitorPipeline(self._roam, self._claude)
        return pipeline.run(deep=True)

    def _run_surveyor(self):
        from living_graph.surveyor import SurveyorPipeline
        pipeline = SurveyorPipeline(self._roam, self._claude, data_dir=self._data_dir)
        return pipeline.survey()
