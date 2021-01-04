# Henry's Python API

To

- Get all projects
- Get all models in a project
- Get all, used, unused explores in a model
- Get all, used, unused fields in an explore
- ... etc 

And

- Analyze models
- Analyze explores

And

- Vacuum models
- Vacuum explores

## Getting started

Henry's interface to Looker API.
 

```python
config = {
    "client_id": "",
    "client_secret": "",
    "host": "",
    "port": 19999,
    "timeout": 500
}

from henry.api import Henry

h = Henry(**config)
```
That's all. It provides access to the following:

```python
>>> h.*
h.analyze( h.analyzer h.args     h.api   h.pulse    h.scanner  h.sdk      h.to_df(   h.vacuum(
```
```python
>>> h.api.*
h.api.api_logger               h.api.get_models(              h.api.host                     h.api.session_info
h.api.auth(                    h.api.get_project(             h.api.id                       h.api.test_connection(
h.api.get_connections(         h.api.get_project_files(       h.api.port                     h.api.timeout
h.api.get_explore(             h.api.get_projects(            h.api.run_git_connection_test( h.api.update_session(
h.api.get_integrations(        h.api.get_session(             h.api.run_inline_query(
h.api.get_legacy_features(     h.api.get_version(             h.api.secret
h.api.get_model(               h.api.git_connection_tests(    h.api.session
```
```python
>>> h.pulse.*
h.pulse.bar                    h.pulse.get_query_stats(
h.pulse.check_connections(     h.pulse.get_query_type_count(
h.pulse.check_integrations(    h.pulse.get_slow_queries(
h.pulse.check_legacy_features( h.pulse.looker
h.pulse.check_query_stats(     h.pulse.postfix_default
h.pulse.check_scheduled_plans( h.pulse.pulse_logger
h.pulse.check_version(         h.pulse.run_all(
```
```python
>>> h.analyzer.*
h.analyzer.analyze(                 h.analyzer.get_explores(            h.analyzer.get_used_explore_fields( h.analyzer.test_git_connection(
h.analyzer.analyze_logger           h.analyzer.get_models(              h.analyzer.get_used_explores(
h.analyzer.fetch_logger             h.analyzer.get_project_files(       h.analyzer.get_used_models(
h.analyzer.get_explore_fields(      h.analyzer.get_unused_explores(     h.analyzer.looker
```
```python
>>> h.scanner.*
h.scanner.fetch_logger             h.scanner.get_project_files(       h.scanner.get_used_models(         h.scanner.vacuum_logger
h.scanner.get_explore_fields(      h.scanner.get_unused_explores(     h.scanner.looker
h.scanner.get_explores(            h.scanner.get_used_explore_fields( h.scanner.test_git_connection(
h.scanner.get_models(              h.scanner.get_used_explores(       h.scanner.vacuum(
```

## Example usage:

```python
from henry.api import Henry
creds = {...}
henry = Henry(**creds)

# -- Looker api
looker = henry.api

projects = looker.get_projects()
models = looker.get_models()
files = looker.get_project_files(project='name')

# -- Analyze (numbers)
analyzer = henry.analyzer
# explores
explores = analyzer.get_explores()
used_explores = analyzer.get_used_explores()
unused_explores = analyzer.get_unused_explores()
# fields
fields = analyzer.get_explore_fields(explore='name')
used_fields = analyzer.get_used_explore_fields()
# full reports
analyze_explores = henry.analyze(which='explores', model='', explore='')
analyze_models = henry.analyze(which='models')

# -- Scan (names)
scanner = henry.scanner
# explores
explores_ = scanner.get_explores()
used_explores_ = scanner.get_used_explores()
unused_explores_ = scanner.get_unused_explores()
# fields
fields_ = scanner.get_explore_fields()
used_fields_ = scanner.get_used_explore_fields()
# full reports
vacuum_explores = henry.vacuum(which='explores', model='', explore='')
vacuum_models = henry.vacuum(which='models')
```