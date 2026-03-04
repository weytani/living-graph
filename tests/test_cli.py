# ABOUTME: Tests for the CLI entry point.
# ABOUTME: Verifies command-line argument parsing and help output.

import subprocess
import sys


def test_cli_help():
    """CLI should show help text."""
    result = subprocess.run(
        [sys.executable, "-m", "living_graph", "--help"],
        capture_output=True,
        text=True,
        cwd="/Users/djr/code/living-graph",
    )
    assert result.returncode == 0
    assert "curate" in result.stdout


def test_cli_curate_help():
    """Curate subcommand should show help."""
    result = subprocess.run(
        [sys.executable, "-m", "living_graph", "curate", "--help"],
        capture_output=True,
        text=True,
        cwd="/Users/djr/code/living-graph",
    )
    assert result.returncode == 0
    assert "--page" in result.stdout or "--date" in result.stdout


def test_cli_janitor_help():
    """Janitor subcommand should show help."""
    result = subprocess.run(
        [sys.executable, "-m", "living_graph", "janitor", "--help"],
        capture_output=True,
        text=True,
        cwd="/Users/djr/code/living-graph",
    )
    assert result.returncode == 0
    assert "--namespace" in result.stdout


def test_cli_distill_help():
    """Distill subcommand should show help."""
    result = subprocess.run(
        [sys.executable, "-m", "living_graph", "distill", "--help"],
        capture_output=True,
        text=True,
        cwd="/Users/djr/code/living-graph",
    )
    assert result.returncode == 0
    assert "--page" in result.stdout or "--date" in result.stdout


def test_cli_survey_help():
    """Survey subcommand should show help."""
    result = subprocess.run(
        [sys.executable, "-m", "living_graph", "survey", "--help"],
        capture_output=True,
        text=True,
        cwd="/Users/djr/code/living-graph",
    )
    assert result.returncode == 0
    assert "--namespace" in result.stdout
    assert "--data-dir" in result.stdout


def test_cli_shows_all_subcommands():
    """Main help should list all worker subcommands."""
    result = subprocess.run(
        [sys.executable, "-m", "living_graph", "--help"],
        capture_output=True,
        text=True,
        cwd="/Users/djr/code/living-graph",
    )
    assert result.returncode == 0
    assert "curate" in result.stdout
    assert "janitor" in result.stdout
    assert "distill" in result.stdout
    assert "survey" in result.stdout
    assert "run" in result.stdout


def test_cli_run_help():
    """Run subcommand should show help."""
    result = subprocess.run(
        [sys.executable, "-m", "living_graph", "run", "--help"],
        capture_output=True,
        text=True,
        cwd="/Users/djr/code/living-graph",
    )
    assert result.returncode == 0
    assert "--date" in result.stdout
    assert "--catch-up" in result.stdout
    assert "--data-dir" in result.stdout
