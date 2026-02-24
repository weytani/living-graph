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
        self._base_url = self.BASE_URL

    def _url(self, endpoint: str) -> str:
        return f"{self._base_url}/api/graph/{self.graph}/{endpoint}"

    def _request(self, endpoint: str, payload: dict) -> dict:
        """Make a request with retry on 429."""
        for attempt in range(self._max_retries + 1):
            resp = self._session.post(self._url(endpoint), json=payload, allow_redirects=True)
            if resp.status_code == 200:
                # Write endpoints return 200 with empty body
                if not resp.content or not resp.content.strip():
                    return {}
                return resp.json()
            if resp.status_code == 429:
                wait = min(2 ** attempt * 5, 65)
                time.sleep(wait)
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
