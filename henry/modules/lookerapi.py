# -*- coding: UTF-8 -*-
import requests
import json
import sys
import logging
import logging.config
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
logging.getLogger("urllib3").setLevel(logging.WARNING)


class LookerApi(object):
    def __init__(self, id, secret, host, port, timeout, session_info):
        self.api_logger = logging.getLogger('lookerapi')
        self.id = id
        self.secret = secret
        self.host = host
        self.port = port
        self.timeout = timeout
        self.session_info = session_info

        self.session = requests.Session()
        self.session.verify = False

        self.auth()

    def auth(self):
        self.api_logger.info('Authenticating')
        url = f'https://{self.host}:{self.port}/api/3.0/login'
        params = {'client_id': self.id, 'client_secret': self.secret}
        self.api_logger.info('Request to %s => POST /api/3.0/login, %s',
                             self.host, {'client_id': params['client_id'],
                                         'client_secret': "[FILTERED]"})
        r = self.session.post(url, params=params, timeout=self.timeout)
        access_token = r.json().get('access_token')
        self.session.headers.update({'Authorization': f'Bearer {access_token}', 
                                     'User-Agent': self.session_info})
        if r.status_code == requests.codes.ok:
            self.api_logger.info('Request Complete: %s', r.status_code)
        else:
            self.api_logger.warning('Request Complete: %s', r.status_code)
            print('Authentication Error: Check supplied credentials.')
            sys.exit(1)

        return

# GET /lookml_models/
    def get_models(self, fields={}):
        url = 'https://{}:{}/api/3.0/{}'.format(self.host,
                                                self.port,
                                                'lookml_models')
        params = fields
        self.api_logger.info('Request to %s => GET /api/3.0/lookml_models, %s',
                             self.host,
                             params)
        r = self.session.get(url, params=params, timeout=self.timeout)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            self.api_logger.error('Request Complete: %s', r.status_code)
            raise(e)
        self.api_logger.info('Request Complete: %s', r.status_code)
        return r.json()

# GET /lookml_models/{{NAME}}
    def get_model(self, model_name=None, fields={}):
        url = 'https://{}:{}/api/3.0/{}/{}'.format(self.host,
                                                   self.port,
                                                   'lookml_models',
                                                   model_name)
        params = fields
        self.api_logger.info('Request to %s => GET /api/3.0/lookml_models/%s,'
                             ' %s', self.host, model_name, params)
        r = self.session.get(url, params=params, timeout=self.timeout)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            self.api_logger.error('Request Complete: %s', r.status_code)
            raise(e)
        self.api_logger.info('Request Complete: %s', r.status_code)
        return [r.json()]

# GET /lookml_models/{{NAME}}/explores/{{NAME}}
    def get_explore(self, model_name=None, explore_name=None, fields={}):
        url = 'https://{}:{}/api/3.0/{}/{}/{}/{}'.format(self.host,
                                                         self.port,
                                                         'lookml_models',
                                                         model_name,
                                                         'explores',
                                                         explore_name)
        params = fields
        self.api_logger.info('Request to %s => GET /api/3.0/lookml_models/%s'
                             '/explores/%s, %s', self.host, model_name,
                             explore_name, params)
        r = self.session.get(url, params=params, timeout=self.timeout)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            self.api_logger.error('Request Complete: %s', r.status_code)
            return []
        self.api_logger.info('Request Complete: %s', r.status_code)
        return [r.json()]

# GET /projects
    def get_projects(self, fields={}):
        url = 'https://{}:{}/api/3.0/{}'.format(self.host,
                                                self.port,
                                                'projects')
        params = fields
        self.api_logger.info('Request to %s => GET /api/3.0/projects, %s',
                             self.host,
                             params)
        r = self.session.get(url, params=params, timeout=self.timeout)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            self.api_logger.warning('Request Complete: %s', r.status_code)
            raise(e)
        self.api_logger.info('Request Complete: %s', r.status_code)
        return r.json()

# GET /projects/{project_id}
    def get_project(self, project_id=None, fields={}):
        url = 'https://{}:{}/api/3.0/{}/{}'.format(self.host,
                                                   self.port,
                                                   'projects',
                                                   project_id)
        params = fields
        self.api_logger.info('Request to %s => GET /api/3.0/projects/%s, %s',
                             self.host,
                             project_id,
                             params)
        r = self.session.get(url, params=params, timeout=self.timeout)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            self.api_logger.error('Request Complete: %s', r.status_code)
            raise e
        self.api_logger.info('Request Complete: %s', r.status_code)
        return [r.json()]

# GET /projects/{project_id}/files
    def get_project_files(self, project=None, fields={}):
        url = 'https://{}:{}/api/3.0/{}/{}/{}'.format(self.host,
                                                      self.port,
                                                      'projects',
                                                      project,
                                                      'files')
        params = fields
        self.api_logger.info('Request to %s => GET /api/3.0/projects/%s/files,'
                             ' %s', self.host, project, params)
        r = self.session.get(url, params=params, timeout=self.timeout)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            self.api_logger.warning('Request Complete: %s', r.status_code)
            print('Project not found: %s', project)
            return
        self.api_logger.info('Request Complete: %s', r.status_code)
        return r.json()

# POST /queries/run/{result_format}
    def run_inline_query(self, result_format, body, fields={}):
        url = 'https://{}:{}/api/3.0/{}/{}/{}'.format(self.host,
                                                      self.port,
                                                      'queries',
                                                      'run',
                                                      result_format)
        params = fields
        self.api_logger.info('Request to %s => POST /api/3.0/queries/run/%s, '
                             '%s', self.host, result_format, params)
        self.api_logger.info('Query params=%s', body)
        r = self.session.post(url, json.dumps(body), params=params,
                              timeout=self.timeout)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            self.api_logger.warning('Request Complete: %s', r.status_code)
            print("Error: " + str(e))
            return
        self.api_logger.info('Request Complete: %s', r.status_code)
        return r.json()

# PATCH session
    def update_session(self, mode):
        url = 'https://{}:{}/api/3.0/{}'.format(self.host,
                                                self.port,
                                                'session')
        body = {'workspace_id': str(mode)}
        self.api_logger.info('Request to %s => PATCH /api/3.0/session, %s',
                             self.host,
                             body)
        r = self.session.patch(url, json=body, timeout=self.timeout)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            self.api_logger.warning('Request Complete: %s', r.status_code)
            print("Error: " + str(e))
            return
        self.api_logger.info('Request Complete: %s', r.status_code)
        return r.json()

# GET session
    def get_session(self, fields={}):
        url = 'https://{}:{}/api/3.0/{}'.format(self.host,
                                                self.port,
                                                'session')
        params = fields
        self.api_logger.info('Request to %s => GET /api/3.0/session, %s',
                             self.host,
                             params)
        r = self.session.get(url, timeout=self.timeout)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            self.api_logger.warning('Request Complete: %s', r.status_code)
            print("Error: " + str(e))
            return
        self.api_logger.info('Request Complete: %s', r.status_code)
        return r.json()

# GET /projects/{project_id}/git_connection_tests
    def git_connection_tests(self, project_id, fields={}):
        url = ('https://{}:{}/api/3.0/projects/{}/'
               'git_connection_tests').format(self.host, self.port, project_id)
        params = fields
        self.api_logger.info('Request to %s => GET /api/3.0/projects/%s/'
                             'git_connection_tests, %s',
                             self.host,
                             project_id,
                             params)
        r = self.session.get(url, timeout=self.timeout)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            self.api_logger.warning('Request Complete: %s', r.status_code)
            print("Error: " + str(e))
            return
        self.api_logger.info('Request Complete: %s', r.status_code)
        return r.json()

# GET /projects/{project_id}/git_connection_tests/{test_id}
    def run_git_connection_test(self, project_id, test_id, fields={}):
        url = ('https://{}:{}/api/3.0/projects/{}/git_connection_tests/'
               '{}').format(self.host, self.port, project_id, test_id)
        params = fields
        self.api_logger.info('Request to %s => GET /api/3.0/projects/%s/'
                             'git_connection_tests/%s, %s',
                             self.host,
                             project_id,
                             test_id,
                             params)
        r = self.session.get(url, timeout=self.timeout)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            self.api_logger.warning('Request Complete: %s', r.status_code)
            print("Error: " + str(e))
            return
        self.api_logger.info('Request Complete: %s', r.status_code)
        return r.json()

# GET /connections
    def get_connections(self, fields={}):
        url = 'https://{}:{}/api/3.0/connections'.format(self.host, self.port)
        params = fields
        self.api_logger.info('Request to %s => GET /api/3.0/connections, %s',
                             self.host,
                             params)
        r = self.session.get(url, timeout=self.timeout)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            self.api_logger.warning('Request Complete: %s', r.status_code)
            return
        self.api_logger.info('Request Complete: %s', r.status_code)
        return r.json()

# PUT /connections/{connection_name}/test
    def test_connection(self, connection, fields={}):
        url = 'https://{}:{}/api/3.0/connections/{}/test'.format(self.host,
                                                                 self.port,
                                                                 connection)
        params = fields
        self.api_logger.info('Request to %s => POST /api/3.0/connections/'
                             '%s/test, %s', self.host, connection, params)
        r = self.session.put(url, params=params, timeout=self.timeout)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            self.api_logger.warning('Request Complete: %s', r.status_code)
            return
        self.api_logger.info('Request Complete: %s', r.status_code)
        return r.json()

# GET /legacy_features
    def get_legacy_features(self, fields={}):
        url = 'https://{}:{}/api/3.0/legacy_features'.format(self.host,
                                                             self.port)
        params = fields
        self.api_logger.info('Request to %s => POST /api/3.0/legacy_features,'
                             ' %s', self.host, params)
        r = self.session.get(url, timeout=self.timeout)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            self.api_logger.warning('Request Complete: %s', r.status_code)
            return
        self.api_logger.info('Request Complete: %s', r.status_code)
        return r.json()

# GET /integrations
    def get_integrations(self, fields={}):
        url = 'https://{}:{}/api/3.0/integrations'.format(self.host, self.port)
        params = fields
        self.api_logger.info('Request to %s => POST /api/3.0/integrations, %s',
                             self.host,
                             params)
        r = self.session.get(url, timeout=self.timeout)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            self.api_logger.warning('Request Complete: %s', r.status_code)
            return
        self.api_logger.info('Request Complete: %s', r.status_code)
        return r.json()

# GET /versions
    def get_version(self, fields={}):
        url = 'https://{}:{}/api/3.0/versions'.format(self.host, self.port)
        params = fields
        self.api_logger.info('Request to %s => POST /api/3.0/versions, %s',
                             self.host,
                             params)
        r = self.session.get(url, timeout=self.timeout)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            self.api_logger.warning('Request Complete: %s', r.status_code)
            print("Error: " + str(e))
            return
        self.api_logger.info('Request Complete: %s', r.status_code)
        return r.json()
