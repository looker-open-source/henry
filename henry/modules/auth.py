import os
import logging
import yaml
import sys
from .lookerapi import LookerApi

auth_logger = logging.getLogger('auth')


# returns an instanstiated Looker object using the
# credentials supplied by the auth argument group
def authenticate(timeout, session_info, config_path, **kwargs):
    auth_logger.info('Authenticating into Looker API')
    # precedence: --path, global config, default
    cleanpath = kwargs['path'] or config_path
    if kwargs['client_id'] and kwargs['client_secret']:
        auth_logger.info('Fetching auth params passed in CLI')
        host = kwargs['host']
        client_id = kwargs['client_id']
        client_secret = kwargs['client_secret']
        token = None
    else:
        auth_logger.info('Checking permissions for %s', cleanpath)
        st = os.stat(cleanpath)
        ap = oct(st.st_mode)
        if ap != '0o100600':
            print(f'Config file permissions are set to %s and are not strict '
                  'enough. Change to rw------- or 600 and try again.' % ap)
            auth_logger.warning('Config file permissions are %s and not strict'
                                ' enough.' % ap)
            sys.exit(1)
        auth_logger.info('Opening config file from %s' % cleanpath)
        try:
            f = open(cleanpath, 'r')
            params = yaml.safe_load(f)
            f.close()
        except FileNotFoundError as error:
            auth_logger.exception(error, exc_info=False)
            print('ERROR: %s not found' % filepath)
            sys.exit(1)

        try:
            auth_logger.info('Fetching auth credentials from file, %s',
                             cleanpath)
            host = params['hosts'][kwargs['host']]['host']
            client_secret = params['hosts'][kwargs['host']]['secret']
            client_id = params['hosts'][kwargs['host']]['id']
        except KeyError as error:
            auth_logger.error('Auth Error: %s not found' % error,
                              exc_info=False)
            print('ERROR: %s not found' % error)
            sys.exit(1)

    auth_logger.info('auth params=%s', {'host': host,
                                        'port': kwargs['port'],
                                        'client_id': client_id,
                                        'client_secret': "[FILTERED]"})
    looker = LookerApi(host=host,
                       port=kwargs['port'],
                       id=client_id,
                       secret=client_secret,
                       timeout=timeout,
                       session_info=session_info,
                       )
    auth_logger.info('Authentication Successful')

    if kwargs['alias']:
        auth_logger.info('Saving credentials to file: %s', cleanpath)
        with open(cleanpath, 'r') as f:
            params = yaml.safe_load(f)
            params['hosts'][kwargs['alias']] = {}
            params['hosts'][kwargs['alias']]['host'] = host
            params['hosts'][kwargs['alias']]['id'] = client_id
            params['hosts'][kwargs['alias']]['secret'] = client_secret

        with open(cleanpath, 'w') as f:
            yaml.safe_dump(params, f, default_flow_style=False)

        os.chmod(cleanpath, 0o600)

    return looker
