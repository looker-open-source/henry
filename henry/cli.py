#!/usr/bin/env p/Spinnerython3
import argparse
import os
import sys
from .modules.spinner import Spinner
import logging.config
import henry
import uuid
from tabulate import tabulate

from .modules import data_controller as dc
from looker_sdk import client, methods
import csv
from typing import Optional
from . import __version__ as pkg

LOGGING_CONFIG_PATH = os.path.join(
    os.path.dirname(henry.__file__), ".support_files/logging.conf"
)
METADATA_PATH = os.path.join(os.path.expanduser("~"), ".henry")
if not os.path.exists(METADATA_PATH):
    os.mkdir(METADATA_PATH)
elif os.path.exists(METADATA_PATH) and not os.path.isdir(METADATA_PATH):
    print("Cannot create metadata directory in %s" % METADATA_PATH)
    sys.exit(1)
LOGGING_LOG_PATH = os.path.join(METADATA_PATH, "log")
if not os.path.exists(LOGGING_LOG_PATH):
    os.mkdir(LOGGING_LOG_PATH)
elif os.path.exists(LOGGING_LOG_PATH) and not os.path.isdir(LOGGING_LOG_PATH):
    print("Cannot create log directory in %s" % LOGGING_LOG_PATH)
    sys.exit(1)
LOGGING_LOG_PATH = os.path.join(LOGGING_LOG_PATH, "henry.log")
logging.config.fileConfig(
    LOGGING_CONFIG_PATH,
    defaults={"logfilename": LOGGING_LOG_PATH},
    disable_existing_loggers=False,
)
from .commands.analyze import Analyze
from .commands.vacuum import Vacuum
from .commands.pulse import Pulse

logger = logging.getLogger("main")


def main():
    logger.info("Starting henry")
    parser = setup_cli()
    args = vars(parser.parse_args())

    # run command
    if args["command"] == "pulse":
        Pulse.run()
    elif args["command"] == "analyze":
        pass
    elif args["command"] == "vacuum":
        pass
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
        usage="henry command subcommand " "[subcommand options] [global " "options]",
        allow_abbrev=False,
        add_help=False,
    )

    parser.add_argument("-h", "--help", action="help", help=argparse.SUPPRESS)

    return parser


def setup_subparsers(parser):
    subparsers = parser.add_subparsers(dest="command", help=argparse.SUPPRESS)
    setup_pulse_subparser(subparsers)
    setup_analyze_subparser(parser, subparsers)
    setup_vacuum_subparser(subparsers)


def setup_pulse_subparser(subparsers):
    subparsers.add_parser("pulse", help="pulse help")


def setup_analyze_subparser(parent_parser, subparsers):
    analyze_parser = subparsers.add_parser("analyze")
    analyze_subparsers = analyze_parser.add_subparsers()

    analyze_projects = analyze_subparsers.add_parser("projects")

    analyze_projects.set_defaults(which="projects")
    analyze_projects.add_argument(
        "-p", "--project", type=str, default=None, help="Filter on a project"
    )
    analyze_projects.add_argument(
        "--order_by",
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

    analyze_models = analyze_subparsers.add_parser("models")
    analyze_models.set_defaults(which="models")
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
        "--min_queries", type=int, default=0, help="Query threshold"
    )
    analyze_models.add_argument(
        "--order_by",
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
    analyze_explores.set_defaults(which="explores")
    analyze_explores.add_argument(
        "-model",
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
        "--min_queries", type=int, default=0, help="Query threshold"
    )
    analyze_explores.add_argument(
        "--order_by",
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
    vacuum_parser.set_defaults(which=None)
    vacuum_subparsers = vacuum_parser.add_subparsers()
    vacuum_models = vacuum_subparsers.add_parser("models")
    vacuum_explores = vacuum_subparsers.add_parser("explores")
    vacuum_models.set_defaults(which="models")
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
        "--min_queries",
        type=int,
        default=0,
        help="Vacuum threshold. Explores with less "
        "queries in the given usage period will "
        "be vacuumed. Default: 0 queries.",
    )
    add_common_arguments(vacuum_models)

    vacuum_explores.set_defaults(which="explores")
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
        "--min_queries", type=int, default=0, help="Query threshold"
    )
    add_common_arguments(vacuum_explores)


def add_common_arguments(parser):
    parser.add_argument(
        "--output", type=str, default=None, help="Path to file for saving the output",
    )
    parser.add_argument("-q", "--quiet", action="store_true", help="Silence output")
    parser.add_argument(
        "--plain",
        default=None,
        action="store_true",
        help="Show results in a table format " "without the gridlines",
    )
    parser.add_argument_group("Authentication")
    parser.add_argument("--path", type=str, default="", help=argparse.SUPPRESS)


if __name__ == "__main__":
    main()
