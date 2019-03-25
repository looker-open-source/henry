import logging
from henry.modules.fetcher import Fetcher as fetcher
from henry.modules import data_controller as dc
from tabulate import tabulate
import json


class Analyze(fetcher):
    def __init__(self, looker):
        super().__init__(looker)
        self.analyze_logger = logging.getLogger('analyze')

    def analyze(self, **kwargs):
        format = 'plain' if kwargs['plain'] else 'psql'
        headers = '' if kwargs['plain'] else 'keys'
        p = kwargs['project'] if 'project' in kwargs.keys() else None
        m = kwargs['model'] if 'model' in kwargs.keys() else None
        self.analyze_logger.info('Analyzing %s', kwargs['which'].capitalize())
        if kwargs['which'] == 'projects':
            params = {k: kwargs[k] for k in {'project', 'sortkey', 'limit'}}
            self.analyze_logger.info('analyze projects params=%s', params)
            result = self._analyze_projects(project=p,
                                            sortkey=kwargs['sortkey'],
                                            limit=kwargs['limit'])
        elif kwargs['which'] == 'models':
            params = {k: kwargs[k] for k in {'project',
                                             'model',
                                             'timeframe',
                                             'min_queries',
                                             'sortkey',
                                             'limit'}}
            self.analyze_logger.info('analyze models params=%s', params)
            result = self._analyze_models(project=p,
                                          model=m,
                                          sortkey=kwargs['sortkey'],
                                          limit=kwargs['limit'],
                                          timeframe=kwargs['timeframe'],
                                          min_queries=kwargs['min_queries'])
        elif kwargs['which'] == 'explores':
            params = {k: kwargs[k] for k in {'model',
                                             'explore',
                                             'timeframe',
                                             'min_queries',
                                             'sortkey',
                                             'limit'}}
            self.analyze_logger.info('analyze explores params=%s', )
            result = self._analyze_explores(model=m,
                                            explore=kwargs['explore'],
                                            sortkey=kwargs['sortkey'],
                                            limit=kwargs['limit'],
                                            timeframe=kwargs['timeframe'],
                                            min_queries=kwargs['min_queries'])
        self.analyze_logger.info('Analyze Complete')

        return result

    def _analyze_projects(self, project=None, sortkey=None, limit=None):
        projects = fetcher.get_project_files(self, project=project)
        info = []
        for p in projects:
            metadata = list(map(lambda x:
                                'model' if x['type'] == 'model' else
                                ('view' if x['type'] == 'view' else None),
                                p['files']))

            model_count = metadata.count('model')
            view_count = metadata.count('view')
            git_tests = fetcher.test_git_connection(self, p['name'])
            info.append({
                'project': p['name'],
                'model_count': model_count,
                'view_count': view_count,
                'git_connection_status': git_tests,
                'pull_request_mode': p['pr_mode'],
                'validation_required': p['validation_required']
            })

        valid_values = list(info[0].keys())
        info = dc.sort(info, valid_values, sortkey)
        info = dc.limit(info, limit=limit)

        return info

    def _analyze_models(self, project=None, model=None,
                        sortkey=None, limit=None,
                        timeframe=90, min_queries=0):
        models = fetcher.get_models(self, project=project,
                                    model=model, verbose=1)
        used_models = fetcher.get_used_models(self, timeframe, min_queries)
        info = []
        for m in models:
            explore_count = len(m['explores'])
            if m['name'] in used_models:
                query_run_count = used_models[m['name']]
            else:
                query_run_count = 0
            unused_explores = fetcher.get_unused_explores(self, m['name'],
                                                          timeframe,
                                                          min_queries)
            info.append({
                'project': m['project_name'],
                'model': m['name'],
                'explore_count': explore_count,
                'unused_explores': len(unused_explores),
                'query_run_count': query_run_count
            })
        valid_values = list(info[0].keys())
        info = dc.sort(info, valid_values, sortkey)
        info = dc.limit(info, limit=limit)
        return info

    def _analyze_explores(self, model=None, explore=None,
                          sortkey=None, limit=None,
                          min_queries=0, timeframe=90):
        explores = fetcher.get_explores(self, model=model,
                                        explore=explore, verbose=1)
        explores_usage = {}
        info = []
        for e in explores:
            # in case explore does not exist (bug - #32748)
            if e is None:
                pass
            else:
                _used_fields = fetcher.get_used_explore_fields(self,
                                                               e['model_name'],
                                                               e['scopes'],
                                                               timeframe,
                                                               min_queries)
                used_fields = list(_used_fields.keys())
                exposed_fields = fetcher.get_explore_fields(self,
                                                            explore=e,
                                                            scoped_names=1)
                unused_fields = set(exposed_fields) - set(used_fields)
                field_count = len(exposed_fields)
                query_count = fetcher.get_used_explores(self,
                                                        model=e['model_name'],
                                                        explore=e['name'])

                all_joins = set(e['scopes'])
                all_joins.remove(e['name'])
                used_joins = set([i.split('.')[2] for i in used_fields])
                unused_joins = len(list(all_joins - used_joins))

                has_description = 'Yes' if e['description'] else 'No'

                if query_count.get(e['name']):
                    query_count = query_count[e['name']]
                else:
                    query_count = 0
                info.append({
                    'model': e['model_name'],
                    'explore': e['name'],
                    'is_hidden': e['hidden'],
                    'has_description': has_description,
                    'join_count': len(all_joins),
                    'unused_joins': unused_joins,
                    'field_count': field_count,
                    'unused_fields': len(unused_fields),
                    'query_count': query_count
                })

        if not info:
            self.analyze_logger.error('No matching explores found')
            raise Exception('No matching explores found')
        valid_values = list(info[0].keys())
        info = dc.sort(info, valid_values, sortkey)
        info = dc.limit(info, limit=limit)
        return info
