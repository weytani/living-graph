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
    results = roam.q(
        '[:find ?uid ?title :where [?p :node/title ?title] [?p :block/uid ?uid] [(clojure.string/starts-with? ?title "Test/")]]'
    )
    for uid, title in results:
        roam.delete_page(uid)
