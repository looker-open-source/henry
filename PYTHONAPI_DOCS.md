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
creds = {
    "client_id": "",
    "client_secret": "",
    "host": "<HOST-URL>",
    "port": 19999,
    "timeout": 500
}

from henry.api import Henry

h = Henry(**creds)
```
That's all. It provides access to the following:

```python
>>> h.*
h.analyze( h.analyzer h.args     h.looker   h.pulse    h.scanner  h.sdk      h.to_df(   h.vacuum(
```
```python
>>> h.looker.*
h.looker.api_logger               h.looker.get_models(              h.looker.host                     h.looker.session_info
h.looker.auth(                    h.looker.get_project(             h.looker.id                       h.looker.test_connection(
h.looker.get_connections(         h.looker.get_project_files(       h.looker.port                     h.looker.timeout
h.looker.get_explore(             h.looker.get_projects(            h.looker.run_git_connection_test( h.looker.update_session(
h.looker.get_integrations(        h.looker.get_session(             h.looker.run_inline_query(
h.looker.get_legacy_features(     h.looker.get_version(             h.looker.secret
h.looker.get_model(               h.looker.git_connection_tests(    h.looker.session
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
looker = henry.looker

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