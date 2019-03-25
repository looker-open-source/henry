#!/usr/bin/env python3
from .modules.lookerapi import LookerApi
import argparse
import os
import errno
import sys
from .modules.spinner import Spinner
from .modules.auth import authenticate
import logging.config
import henry
from pathlib import PosixPath
import json
import uuid
from tabulate import tabulate
from .modules import data_controller as dc
import csv
from . import __version__ as pkg
LOGGING_CONFIG_PATH = os.path.join(os.path.dirname(henry.__file__),
                                   '.support_files/logging.conf')
METADATA_PATH = os.path.join(os.path.expanduser('~'), '.henry')
if not os.path.exists(METADATA_PATH):
    os.mkdir(METADATA_PATH)
elif os.path.exists(METADATA_PATH) and not os.path.isdir(METADATA_PATH):
    print('Cannot create metadata directory in %s' % METADATA_PATH)
    sys.exit(1)
LOGGING_LOG_PATH = os.path.join(METADATA_PATH, 'log')
if not os.path.exists(LOGGING_LOG_PATH):
    os.mkdir(LOGGING_LOG_PATH)
elif os.path.exists(LOGGING_LOG_PATH) and not os.path.isdir(LOGGING_LOG_PATH):
    print('Cannot create log directory in %s' % LOGGING_LOG_PATH)
    sys.exit(1)
LOGGING_LOG_PATH = os.path.join(LOGGING_LOG_PATH, 'henry.log')
logging.config.fileConfig(LOGGING_CONFIG_PATH,
                          defaults={'logfilename': LOGGING_LOG_PATH},
                          disable_existing_loggers=False)
from .commands.analyze import Analyze
from .commands.vacuum import Vacuum
from .commands.pulse import Pulse

logger = logging.getLogger('main')
# sys.tracebacklimit = -1 # enable only on shipped release


def main():
    logger.info('Starting henry')
    HELP_PATH = os.path.join(os.path.dirname(henry.__file__),
                             '.support_files/help.rtf')
    with open(HELP_PATH, 'r', encoding='unicode_escape') as myfile:
        descStr = myfile.read()

    # load custom config settings if defined in ~/.henry/henry.json
    settings_file = PosixPath(os.path.join(METADATA_PATH, 'settings.json')).expanduser()
    timeout = 120
    config_path = PosixPath.cwd().joinpath('config.yml')
    if settings_file.is_file():
        with open(settings_file, 'r') as f:
            settings = json.load(f)
            timeout = settings.get('api_conn_timeout', timeout)
            if type(timeout) is list:
                timeout = tuple(timeout)
            config_path = settings.get('config_path', config_path)
        logger.info(f'Loaded config settings from ~/.henry/settings.json, {settings}')
    else:
        logger.info('No custom config file found. Using defaults.')

    parser = argparse.ArgumentParser(
        description=descStr,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        prog='henry',
        usage='henry command subcommand '
              '[subcommand options] [global '
              'options]',
        allow_abbrev=False,
        add_help=False)

    subparsers = parser.add_subparsers(dest='command',
                                       help=argparse.SUPPRESS)
    parser.add_argument("-h", "--help", action="help", help=argparse.SUPPRESS)

    # subparsers.required = True # works, but might do without for now.

    pulse = subparsers.add_parser('pulse', help='pulse help')

    analyze_parser = subparsers.add_parser('analyze', help='analyze help',
                                           usage='henry analyze')
    analyze_parser.set_defaults(which=None)
    analyze_subparsers = analyze_parser.add_subparsers()
    analyze_projects = analyze_subparsers.add_parser('projects')
    analyze_models = analyze_subparsers.add_parser('models')
    analyze_explores = analyze_subparsers.add_parser('explores')

    # project subcommand
    analyze_projects.set_defaults(which='projects')
    analyze_projects.add_argument('-p', '--project',
                                  type=str,
                                  default=None,
                                  help='Filter on a project')
    analyze_projects.add_argument('--order_by',
                                  nargs=2,
                                  metavar=('ORDER_FIELD', 'ASC/DESC'),
                                  dest='sortkey',
                                  help='Sort results by a field')
    analyze_projects.add_argument('--limit',
                                  type=int,
                                  default=None,
                                  nargs=1,
                                  help='Limit results. No limit by default')

    # models subcommand
    analyze_models.set_defaults(which='models')
    models_group = analyze_models.add_mutually_exclusive_group()

    models_group.add_argument('-p', '--project',
                              type=str,
                              default=None,
                              help='Filter on project')
    models_group.add_argument('-model', '--model',
                              type=str,
                              default=None,
                              help='Filter on model')
    analyze_models.add_argument('--timeframe',
                                type=int,
                                default=90,
                                help='Timeframe (between 0 and 90)')
    analyze_models.add_argument('--min_queries',
                                type=int,
                                default=0,
                                help='Query threshold')
    analyze_models.add_argument('--order_by',
                                nargs=2,
                                metavar=('ORDER_FIELD', 'ASC/DESC'),
                                dest='sortkey',
                                help='Sort results by a field')
    analyze_models.add_argument('--limit',
                                type=int,
                                default=None,
                                nargs=1,
                                help='Limit results. No limit by default')

    # explores subcommand
    analyze_explores.set_defaults(which='explores')
    analyze_explores.add_argument('-model', '--model',
                                  type=str,
                                  default=None,
                                  required=('--explore') in sys.argv,
                                  help='Filter on model')
    analyze_explores.add_argument('-e', '--explore',
                                  default=None,
                                  help='Filter on model')
    analyze_explores.add_argument('--timeframe',
                                  type=int,
                                  default=90,
                                  help='Timeframe (between 0 and 90)')
    analyze_explores.add_argument('--min_queries',
                                  type=int,
                                  default=0,
                                  help='Query threshold')
    analyze_explores.add_argument('--order_by',
                                  nargs=2,
                                  metavar=('ORDER_FIELD', 'ASC/DESC'),
                                  dest='sortkey',
                                  help='Sort results by a field')
    analyze_explores.add_argument('--limit',
                                  type=int,
                                  default=None,
                                  nargs=1,
                                  help='Limit results. No limit by default')

    # VACUUM Subcommand
    vacuum_parser = subparsers.add_parser('vacuum', help='vacuum help',
                                          usage='henry vacuum')
    vacuum_parser.set_defaults(which=None)
    vacuum_subparsers = vacuum_parser.add_subparsers()
    vacuum_models = vacuum_subparsers.add_parser('models')
    vacuum_explores = vacuum_subparsers.add_parser('explores')
    vacuum_models.set_defaults(which='models')
    vm_group = vacuum_models.add_mutually_exclusive_group()
    vm_group.add_argument('-p', '--project',
                          type=str,
                          default=None,
                          help='Filter on Project')
    vm_group.add_argument('-m', '--model',
                          type=str,
                          default=None,
                          help='Filter on model')

    vacuum_models.add_argument('--timeframe',
                               type=int,
                               default=90,
                               help='Usage period to examine (in the range of '
                                    '0-90 days). Default: 90 days.')

    vacuum_models.add_argument('--min_queries',
                               type=int,
                               default=0,
                               help='Vacuum threshold. Explores with less '
                                    'queries in the given usage period will '
                                    'be vacuumed. Default: 0 queries.')

    vacuum_explores.set_defaults(which='explores')
    vacuum_explores.add_argument('-m', '--model',
                                 type=str,
                                 default=None,
                                 required=('--explore') in sys.argv,
                                 help='Filter on model')

    vacuum_explores.add_argument('-e', '--explore',
                                 type=str,
                                 default=None,
                                 help='Filter on explore')

    vacuum_explores.add_argument('--timeframe',
                                 type=int,
                                 default=90,
                                 help='Timeframe (between 0 and 90)')

    vacuum_explores.add_argument('--min_queries',
                                 type=int,
                                 default=0,
                                 help='Query threshold')

    for subparser in [analyze_projects, analyze_models, analyze_explores,
                      vacuum_models, vacuum_explores, pulse]:
        subparser.add_argument('--output',
                               type=str,
                               default=None,
                               help='Path to file for saving the output')
        subparser.add_argument('-q', '--quiet',
                               action='store_true',
                               help='Silence output')
        subparser.add_argument('--plain',
                               default=None,
                               action='store_true',
                               help='Show results in a table format '
                                    'without the gridlines')
        subparser.add_argument_group("Authentication")
        subparser.add_argument('--host', type=str, default='looker',
                               required=any(k in sys.argv for k in
                                            ['--client_id', '--client_secret',
                                             '--alias']),
                               help=argparse.SUPPRESS)
        subparser.add_argument('--port', type=int, default=19999,
                               help=argparse.SUPPRESS)
        subparser.add_argument('--client_id', type=str,
                               required=any(k in sys.argv for k in
                                            ['--client_secret', '--alias']),
                               help=argparse.SUPPRESS)
        subparser.add_argument('--client_secret', type=str,
                               required=any(k in sys.argv for k in
                                            ['--client_id', '--alias']),
                               help=argparse.SUPPRESS)
        subparser.add_argument('--alias', type=str,
                               help=argparse.SUPPRESS)
        subparser.add_argument('--path', type=str, default='',
                               help=argparse.SUPPRESS)

    args = vars(parser.parse_args())
    _args = {}
    for key, value in args.items():
        if key == 'client_secret':
            _args[key] = '[FILTERED]'
        else:
            _args[key] = value
    logger.info('Parsing args, %s', _args)

    if not args['command']:
        print('usage:', parser.usage)
        print('\nNo command specified. Try `henry --help` for help.')
        sys.exit(1)
    auth_params = ('host', 'port', 'client_id', 'client_secret',
                   'alias', 'path')
    auth_args = {k: args[k] for k in auth_params}

    # authenticate
    if args['command'] != 'pulse':
        cmd = args['command']+' '+args['which']
    else:
        cmd = args['command']
    session_info = f'Henry v{pkg.__version__}: cmd={cmd}' \
                   f', sid=#{uuid.uuid1()}'
    looker = authenticate(timeout, session_info, config_path, **auth_args)

    # map subcommand to function
    if args['command'] in ('analyze', 'vacuum'):
        if args['which'] is None:
            parser.error("No command")
        else:
            with Spinner():
                if args['command'] == 'analyze':
                    analyze = Analyze(looker)
                    result = analyze.analyze(**args)
                else:
                    vacuum = Vacuum(looker)
                    result = vacuum.vacuum(**args)
        # print results if --silence flag is not used
        if not args['quiet']:
            # tabulate result and print
            tablefmt = 'plain' if args['plain'] else 'psql'
            headers = 'keys'
            formatted_result = tabulate(result, headers=headers,
                                        tablefmt=tablefmt, numalign='center')
            print(formatted_result)

    elif args['command'] == 'pulse':
                pulse = Pulse(looker)
                result = pulse.run_all()
    else:
        print('No command passed')

    # save to file if --output flag is used
    if args['output']:
        file = args['output']
        logger.info(f'Saving results to {file}')
        dc.save_to_file(args['output'], result)
        logger.info('Results succesfully saved.')


if __name__ == "__main__":
    main()
