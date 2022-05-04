#!/usr/bin/env
import argparse
import os
import sys

import henry
from henry.commands import analyze, pulse, vacuum
from henry.modules import fetcher


def main():
    parser = setup_cli()
    user_input = parse_input(parser)

    if user_input.command == "pulse":
        pulse.Pulse.run(user_input)
    elif user_input.command == "analyze":
        analyze.Analyze.run(user_input)
    elif user_input.command == "vacuum":
        vacuum.Vacuum.run(user_input)
    else:
        parser.error()


def setup_cli():
    parser = create_parser()
    setup_subparsers(parser)
    return parser


def create_parser():
    help_file = os.path.join(os.path.dirname(henry.__file__), ".support_files/help.rtf")
    with open(help_file, "r", encoding="unicode_escape") as myfile:
        description = myfile.read()

    parser = argparse.ArgumentParser(
        description=description,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        prog="henry",
        usage="henry command subcommand "
        "[subcommand options] [global "
        "options]\n\n",
        allow_abbrev=False,
        add_help=False,
    )

    parser.add_argument("-h", "--help", action="help", help=argparse.SUPPRESS)

    return parser


def setup_subparsers(parser):
    subparsers = parser.add_subparsers(dest="command", help=argparse.SUPPRESS)
    setup_pulse_subparser(subparsers)
    setup_analyze_subparser(subparsers)
    setup_vacuum_subparser(subparsers)


def setup_pulse_subparser(subparsers):
    pulse_parser = subparsers.add_parser(
        "pulse", help="pulse help", usage="henry pulse [global options]"
    )
    pulse_parser.add_argument(
        "--timeout", type=int, default=120, help=argparse.SUPPRESS
    )
    pulse_parser.add_argument_group("Authentication")
    pulse_parser.add_argument(
        "--config-file", type=str, default="looker.ini", help=argparse.SUPPRESS
    )
    pulse_parser.add_argument(
        "--section", type=str, default="Looker", help=argparse.SUPPRESS
    )


def setup_analyze_subparser(subparsers):
    analyze_parser = subparsers.add_parser(
        "analyze", help="analyze help", usage="henry analyze"
    )
    analyze_subparsers = analyze_parser.add_subparsers(dest="subcommand")

    analyze_projects = analyze_subparsers.add_parser("projects")

    analyze_projects.add_argument(
        "-p", "--project", type=str, default=None, help="Filter on a project"
    )
    analyze_projects.add_argument(
        "--order-by",
        nargs=2,
        metavar=("ORDER_FIELD", "ASC/DESC"),
        dest="sortkey",
        help="Sort results by a field",
    )
    analyze_projects.add_argument(
        "--limit",
        type=int,
        default=None,
        nargs=1,
        help="Limit results. No limit by default",
    )
    add_common_arguments(analyze_projects)

    analyze_models = analyze_subparsers.add_parser("models")
    models_group = analyze_models.add_mutually_exclusive_group()

    models_group.add_argument(
        "-p", "--project", type=str, default=None, help="Filter on project"
    )
    models_group.add_argument(
        "-model", "--model", type=str, default=None, help="Filter on model"
    )
    analyze_models.add_argument(
        "--timeframe", type=int, default=90, help="Timeframe, between 0 and 90"
    )
    analyze_models.add_argument(
        "--min-queries", type=int, default=0, help="Query threshold"
    )
    analyze_models.add_argument(
        "--order-by",
        nargs=2,
        metavar=("ORDER_FIELD", "ASC/DESC"),
        dest="sortkey",
        help="Sort results by a field",
    )
    analyze_models.add_argument(
        "--limit",
        type=int,
        default=None,
        nargs=1,
        help="Limit results. No limit by default",
    )
    add_common_arguments(analyze_models)

    analyze_explores = analyze_subparsers.add_parser("explores")
    analyze_explores.add_argument(
        "-m",
        "--model",
        type=str,
        default=None,
        required=("--explore") in sys.argv,
        help="Filter on model",
    )
    analyze_explores.add_argument(
        "-e", "--explore", default=None, help="Filter on model"
    )
    analyze_explores.add_argument(
        "--timeframe", type=int, default=90, help="Timeframe, between 0 and 90"
    )
    analyze_explores.add_argument(
        "--min-queries", type=int, default=0, help="Query threshold"
    )
    analyze_explores.add_argument(
        "--order-by",
        nargs=2,
        metavar=("ORDER_FIELD", "ASC/DESC"),
        dest="sortkey",
        help="Sort results by a field",
    )
    analyze_explores.add_argument(
        "--limit",
        type=int,
        default=None,
        nargs=1,
        help="Limit results. No limit by default",
    )
    add_common_arguments(analyze_explores)


def setup_vacuum_subparser(subparsers):
    vacuum_parser = subparsers.add_parser(
        "vacuum", help="vacuum help", usage="henry vacuum"
    )
    vacuum_subparsers = vacuum_parser.add_subparsers(dest="subcommand")
    vacuum_models = vacuum_subparsers.add_parser("models")
    vacuum_explores = vacuum_subparsers.add_parser("explores")
    vm_group = vacuum_models.add_mutually_exclusive_group()
    vm_group.add_argument(
        "-p", "--project", type=str, default=None, help="Filter on Project"
    )
    vm_group.add_argument(
        "-m", "--model", type=str, default=None, help="Filter on model"
    )

    vacuum_models.add_argument(
        "--timeframe",
        type=int,
        default=90,
        help="Usage period to examine (in the range of "
        "0-90 days). Default: 90 days.",
    )

    vacuum_models.add_argument(
        "--min-queries",
        type=int,
        default=0,
        help="Vacuum threshold. Explores with less "
        "queries in the given usage period will "
        "be vacuumed. Default: 0 queries.",
    )
    add_common_arguments(vacuum_models)

    vacuum_explores.add_argument(
        "-m",
        "--model",
        type=str,
        default=None,
        required=("--explore") in sys.argv,
        help="Filter on model",
    )

    vacuum_explores.add_argument(
        "-e", "--explore", type=str, default=None, help="Filter on explore"
    )

    vacuum_explores.add_argument(
        "--timeframe", type=int, default=90, help="Timeframe, between 0 and 90"
    )

    vacuum_explores.add_argument(
        "--min-queries", type=int, default=0, help="Query threshold"
    )
    add_common_arguments(vacuum_explores)


def add_common_arguments(parser: argparse.ArgumentParser):
    parser.add_argument(
        "--save",
        action="store_true",
        default=False,
        help="Save output to CSV.",
    )
    parser.add_argument("-q", "--quiet", action="store_true", help="Silence output")
    parser.add_argument("--timeout", type=int, default=120, help=argparse.SUPPRESS)
    parser.add_argument_group("Authentication")
    parser.add_argument(
        "--config-file", type=str, default="looker.ini", help=argparse.SUPPRESS
    )
    parser.add_argument("--section", type=str, default="Looker", help=argparse.SUPPRESS)


def parse_input(parser: argparse.ArgumentParser):
    args = vars(parser.parse_args())
    return fetcher.Input(**args)


if __name__ == "__main__":
    main()
