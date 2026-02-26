# ABOUTME: CLI entry point for the living graph system.
# ABOUTME: Run workers from command line: python -m living_graph {curate,janitor,distill,survey}

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


def cmd_distill(args):
    """Run the distiller pipeline."""
    load_dotenv()
    import anthropic
    from living_graph.client import RoamClient
    from living_graph.distiller import DistillerPipeline

    roam = RoamClient(
        graph=os.environ["ROAM_GRAPH"],
        token=os.environ["ROAM_API_TOKEN"],
    )
    claude = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    pipeline = DistillerPipeline(roam, claude)

    if args.page:
        pages = [args.page]
    else:
        target = date.today()
        if args.date:
            target = date.fromisoformat(args.date)
        pages = [_ordinal_date(target)]

        if args.catch_up:
            for i in range(1, args.catch_up + 1):
                d = target - timedelta(days=i)
                pages.append(_ordinal_date(d))

    for page_title in pages:
        print(f"Distilling: {page_title}")
        result = pipeline.distill_page(page_title)
        print(
            f"  Blocks: {result['blocks_processed']}, "
            f"Insights: {result['insights_extracted']}, "
            f"Created: {result['pages_created']}, "
            f"Existing: {result['pages_resolved']}"
        )

    print("Done.")


def cmd_survey(args):
    """Run the surveyor pipeline."""
    load_dotenv()
    import anthropic
    from living_graph.client import RoamClient
    from living_graph.surveyor import SurveyorPipeline

    roam = RoamClient(
        graph=os.environ["ROAM_GRAPH"],
        token=os.environ["ROAM_API_TOKEN"],
    )
    claude = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    data_dir = args.data_dir or "data"
    pipeline = SurveyorPipeline(roam, claude, data_dir=data_dir)

    pages = None
    if args.namespace:
        # Survey only pages in a specific namespace
        results = roam.q(
            '[:find ?title :where '
            '[?p :node/title ?title] '
            '[(clojure.string/starts-with? ?title "' + args.namespace + '")]]'
        )
        pages = sorted(row[0] for row in results)
        print(f"Surveying {len(pages)} pages in {args.namespace}...")
    else:
        print("Surveying all typed pages...")

    result = pipeline.survey(page_titles=pages)

    print(
        f"  Embedded: {result['pages_embedded']}\n"
        f"  Clusters: {result['clusters_found']}\n"
        f"  Tags written: {result['tags_written']}\n"
        f"  Relationships written: {result['relationships_written']}"
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

    distill_parser = subparsers.add_parser("distill", help="Run the distiller pipeline")
    distill_parser.add_argument(
        "--page", type=str, help="Specific page title to distill"
    )
    distill_parser.add_argument(
        "--date", type=str, help="Date to distill (YYYY-MM-DD format, default: today)"
    )
    distill_parser.add_argument(
        "--catch-up",
        type=int,
        default=0,
        help="Also process the previous N days",
    )
    distill_parser.set_defaults(func=cmd_distill)

    survey_parser = subparsers.add_parser("survey", help="Run the surveyor pipeline")
    survey_parser.add_argument(
        "--namespace",
        type=str,
        help="Limit survey to a specific namespace prefix (e.g. Person/)",
    )
    survey_parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        help="Directory for vector DB and state files (default: data/)",
    )
    survey_parser.set_defaults(func=cmd_survey)

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
