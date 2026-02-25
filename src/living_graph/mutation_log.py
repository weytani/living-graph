# ABOUTME: Structured mutation logging for living-graph workers.
# ABOUTME: Creates Run/ pages in Roam with timestamped entries for every graph mutation.

from __future__ import annotations

import json
import time
from datetime import datetime


def _ordinal(day: int) -> str:
    """Return day with ordinal suffix (1st, 2nd, 3rd, 4th, etc.)."""
    if 11 <= day <= 13:
        return f"{day}th"
    suffix = {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")
    return f"{day}{suffix}"


def _roam_date(date_str: str) -> str:
    """Convert YYYY-MM-DD to Roam ordinal format like 'February 24th, 2026'.

    If the date string has extra segments (e.g. '2026-02-24-log'),
    only the first three parts are used.
    """
    parts = date_str.split("-")
    year, month, day = int(parts[0]), int(parts[1]), int(parts[2])
    dt = datetime(year, month, day)
    month_name = dt.strftime("%B")
    return f"{month_name} {_ordinal(day)}, {year}"


class MutationLogger:
    """Logs structured mutations to Run/ pages in Roam."""

    def __init__(self, client, namespace_prefix: str = "Run/"):
        self._roam = client
        self._prefix = namespace_prefix

    def create_run(self, worker: str, date: str) -> dict:
        """Create a new Run page with metadata blocks.

        Args:
            worker: Name of the worker process (e.g. 'EntityResolver').
            date: Date string in YYYY-MM-DD format (may have suffix).

        Returns:
            Dict with 'uid' and 'title' keys.
        """
        timestamp = datetime.now().strftime("%H%M%S")
        title = f"{self._prefix}{worker} {date} {timestamp}"
        roam_date = _roam_date(date)

        # Create the page
        self._roam.create_page(title)
        time.sleep(2)

        # Find the page UID
        results = self._roam.q(
            '[:find ?uid :where '
            '[?p :node/title ?title] '
            '[?p :block/uid ?uid] '
            '[(= ?title "' + title + '")]]'
        )
        if not results:
            raise RuntimeError(f"Failed to find created page: {title}")

        page_uid = results[0][0]

        # Add metadata blocks via batch
        self._roam.batch([
            {
                "action": "create-block",
                "location": {"parent-uid": page_uid, "order": 0},
                "block": {"string": f"Process:: [[{worker}]]"},
            },
            {
                "action": "create-block",
                "location": {"parent-uid": page_uid, "order": 1},
                "block": {"string": f"Date:: [[{roam_date}]]"},
            },
            {
                "action": "create-block",
                "location": {"parent-uid": page_uid, "order": 2},
                "block": {"string": "Status:: running"},
            },
        ])
        time.sleep(1)

        return {"uid": page_uid, "title": title}

    def log(
        self,
        run_uid: str,
        action: str,
        target: str,
        changes: dict,
    ) -> None:
        """Log a single mutation to the run page.

        Creates a child block with format:
            `HH:MM:SS` **action** [[target]] `{json}`
        """
        timestamp = datetime.now().strftime("%H:%M:%S")
        changes_json = json.dumps(changes, ensure_ascii=False)
        block_string = f"`{timestamp}` **{action}** [[{target}]] `{changes_json}`"

        self._roam.create_block(run_uid, block_string, order="last")

    def close_run(
        self,
        run_uid: str,
        status: str = "completed",
        summary: str = "",
    ) -> None:
        """Close a run by updating its Status block and adding a Summary block.

        Finds the existing Status:: block and updates it, then appends
        a Summary:: block.
        """
        # Pull current children to find the Status:: block
        tree = self._roam.pull(
            "[:block/uid :block/string {:block/children [:block/uid :block/string]}]",
            f'[:block/uid "{run_uid}"]',
        )
        children = tree.get(":block/children", [])

        # Find and update the Status:: block
        for child in children:
            text = child.get(":block/string", "")
            if text.startswith("Status::"):
                child_uid = child.get(":block/uid")
                if child_uid:
                    self._roam.update_block(child_uid, f"Status:: {status}")
                break

        # Add summary block
        if summary:
            self._roam.create_block(run_uid, f"Summary:: {summary}", order="last")
