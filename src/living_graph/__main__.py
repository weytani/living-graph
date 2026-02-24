# ABOUTME: CLI entry point for the living graph system.
# ABOUTME: Run workers from command line: python -m living_graph {curate,janitor}

import argparse
import os
import sys
from datetime import date, timedelta

from dotenv import load_dotenv


def _ordinal_date(d: date) -> str:
    """Convert a date to Roam ordinal format: 'February 24th, 2026'."""
    day = d.day
    if 11 <= day <= 13:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")
    return d.strftime(f"%B {day}{suffix}, %Y")


def cmd_curate(args):
    """Run the curator pipeline."""
    load_dotenv()
    import anthropic
    from living_graph.client import RoamClient
    from living_graph.curator import CuratorPipeline

    roam = RoamClient(
        graph=os.environ["ROAM_GRAPH"],
        token=os.environ["ROAM_API_TOKEN"],
    )
    claude = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    pipeline = CuratorPipeline(roam, claude)

    if args.page:
        pages = [args.page]
    else:
        # Default: today's daily page
        target = date.today()
        if args.date:
            target = date.fromisoformat(args.date)
        pages = [_ordinal_date(target)]

        # Catch-up: also process previous N days
        if args.catch_up:
            for i in range(1, args.catch_up + 1):
                d = target - timedelta(days=i)
                pages.append(_ordinal_date(d))

    for page_title in pages:
        print(f"Curating: {page_title}")
        result = pipeline.curate_page(page_title)
        print(
            f"  Blocks: {result['blocks_processed']}, "
            f"Entities: {result['entities_resolved']}, "
            f"Enriched: {result['entities_enriched']}"
        )

    print("Done.")


def cmd_janitor(args):
    """Run the janitor pipeline."""
    load_dotenv()
    import anthropic
    from living_graph.client import RoamClient
    from living_graph.janitor import JanitorPipeline

    roam = RoamClient(
        graph=os.environ["ROAM_GRAPH"],
        token=os.environ["ROAM_API_TOKEN"],
    )

    deep = not args.light
    claude = None
    if deep:
        claude = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    namespaces = [args.namespace] if args.namespace else None
    pipeline = JanitorPipeline(roam, claude)

    mode = "deep sweep" if deep else "light sweep"
    print(f"Running janitor ({mode})...")
    result = pipeline.run(namespaces=namespaces, deep=deep)

    print(
        f"  Mode: {result['mode']}\n"
        f"  Pages scanned: {result['pages_scanned']}\n"
        f"  Issues found: {result['issues_found']}\n"
        f"  Fixed: {result['fixed']}\n"
        f"  Flagged: {result['flagged']}\n"
        f"  Skipped: {result['skipped']}"
    )
    print("Done.")


def main():
    parser = argparse.ArgumentParser(
        prog="living_graph",
        description="Living Graph — Roam maintenance workers",
    )
    subparsers = parser.add_subparsers(dest="command")

    curate_parser = subparsers.add_parser("curate", help="Run the curator pipeline")
    curate_parser.add_argument(
        "--page", type=str, help="Specific page title to curate"
    )
    curate_parser.add_argument(
        "--date", type=str, help="Date to curate (YYYY-MM-DD format, default: today)"
    )
    curate_parser.add_argument(
        "--catch-up",
        type=int,
        default=0,
        help="Also process the previous N days",
    )
    curate_parser.set_defaults(func=cmd_curate)

    janitor_parser = subparsers.add_parser("janitor", help="Run the janitor pipeline")
    janitor_parser.add_argument(
        "--namespace",
        type=str,
        help="Limit scan to a specific namespace prefix (e.g. Person/)",
    )
    janitor_parser.add_argument(
        "--light",
        action="store_true",
        default=False,
        help="Light sweep (Stage 1 only, no LLM calls)",
    )
    janitor_parser.set_defaults(func=cmd_janitor)

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
