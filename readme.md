![image](https://github.com/looker-open-source/henry/blob/master/doc/logo/logo.png?raw=true)

-----------------
# Henry: A Looker Cleanup Tool
Henry is a command line tool that helps determine model bloat in your Looker instance and identify unused content in models and explores. It is meant to help developers cleanup models from unused explores and explores from unused joins and fields, as well as maintain a healthy and user-friendly instance.

## Table of Contents
- [Henry: A Looker Cleanup Tool](#henry-a-looker-cleanup-tool)
  - [Table of Contents](#table-of-contents)
  - [Status and Support](#status-and-support)
  - [Where to get it](#where-to-get-it)
  - [Usage](#usage)
    - [Storing Credentials](#storing-credentials)
    - [Global Config File](#global-config-file)
      - [API timeout settings](#api-timeout-settings)
      - [Config Path](#config-path)
    - [Global Options that apply to many commands](#global-options-that-apply-to-many-commands)
      - [Suppressing Formatted Output](#suppressing-formatted-output)
      - [Output to File](#output-to-file)
    - [Pulse Command](#pulse-command)
      - [Connection Checks](#connection-checks)
      - [Query Stats](#query-stats)
      - [Scheduled Plans](#scheduled-plans)
      - [Legacy Features](#legacy-features)
      - [Version](#version)
    - [Analyze Command](#analyze-command)
      - [analyze projects](#analyze-projects)
      - [analyze models](#analyze-models)
      - [analyze explores](#analyze-explores)
    - [Vacuum Information](#vacuum-information)
      - [vacuum models](#vacuum-models)
      - [vacuum explores](#vacuum-explores)
  - [Logging](#logging)
  - [Dependencies](#dependencies)
  - [Development](#development)
  - [Authors](#authors)
  - [Contributing](#contributing)
  - [Code of Conduct](#code-of-conduct)
  - [Copyright](#copyright)

<a name="status_and_support"></a>
## Status and Support
Henry is **NOT** supported or warranted by Looker in any way. Please do not contact Looker support
for issues with Henry. Issues can be logged via https://github.com/looker-open-source/henry/issues

<a name="where_to_get_it"></a>
## Where to get it
The source code is currently hosted on GitHub at https://github.com/looker-open-source/henry/. The latest released version can be found on [PyPI](https://pypi.org/project/henry/) and can be installed using:

    $ pip install henry

For development setup, follow the Development setup [below](#development).

<a name="usage"></a>
## Usage
In order to display usage information, use:

    $ henry --help

<a name="storing_credentials"></a>
### Storing Credentials
API3 login credentials can be specified at runtime using various flags or more conveniently, using a `config.yml` having the format shown below.

```
hosts:
  dev_looker:
    host: devhostname.looker.com
    id: AbCdEfGhIjKlMnOp
    secret: QrStUvWxYz1234567890
  staging_looker:
    host: staginghostname.looker.com
    id: AbCdEfGhIjKlMnOp
    secret: QrStUvWxYz1234567890
```

Make sure that the `config.yml` file has restricted permissions by running `chmod 600 config.yml`. The tool will also ensure that this is the case every time it writes to the file.

If `config.yml` resides in the current working directory, then you don't need to do anything. If not, its location needs to be specified at runtime using the `--path` parameter or in the [global config file](#global-config-file). 

<a name="global_config_file"></a>
### Global Config File
A global settings file called `settings.json` can be defined in `~/.henry`. The file can be used to define a number of paramaters to be used at runtime:

```
{
    "api_conn_timeout": x,
    "config_path": "/path/to/api3/credentials/yml/file"

}
```
<a name="api_timeout_settings"></a>
#### API timeout settings
The `api_conn_timeout` parameter can be used to specify API call timeout settings. It can take 3 types of values: null, an integer representing
connect and read timeouts (in seconds) combined or a list that specifies
the connect and read timeouts separately (e.g. "[5, 15]").

<a name="config_path"></a>
#### Config Path
The `config_path` parameter defines the absolute location to the [API3 credentials file](#storing-credentials). 

In order of precedence, these are the ways that are used to define the location of the credentials file path:
--path, config_path in ~/.henry/settings.json and then the default.

<a name="global_options"></a>
### Global Options that apply to many commands
<a name="suppressed_output"></a>
#### Suppressing Formatted Output
Many commands provide tabular output. For tables the option `--plain` will suppress the table headers and format lines, making it easier to use tools like grep, awk, etc. to retrieve values from the output of these commands.

<a name="output_to_file"></a>
#### Output to File
Using the `--output` option allows you to specify a path and a file to save the results to. When combined with `--plain` the format lines will be suppressed. Example usage:

    $ henry vacuum models --plain --output=unused_explores.csv

saves the results to *unused_explores.csv* in the current working directory.

<a name="pulse_cmd"></a>
### Pulse Command
The command `henry pulse` runs a number of tests that help determine the overall instance health. A healthy Looker instance should pass all the tests. Below is a list of tests currently implemented.

#### Connection Checks
Runs specific tests for each connection to make sure the connection is in working order. If any tests fail, the output will show which tests passed or failed for that particular connection. Example:
```
+------------------+------------------------------------------------------------------------------------------------------+
| Connection       | Status                                                                                               |
|------------------+------------------------------------------------------------------------------------------------------|
| thelook          | -- Failed to create or write to pdt connection registration table tmp.connection_reg_r3 : Connection |
|                  | registration error for thelook: max registrations reached for connection thelook                     |
| assets_analytics | OK                                                                                                   |
| events_ecommerce | OK                                                                                                   |
+------------------+------------------------------------------------------------------------------------------------------+
```

#### Query Stats
Checks how many queries were run over the past 30 days and how many of them errored or got killed as well as some statistics around runtimes times. The IDs of queries that took more than 5 times the average query runtime are also outputted.

#### Scheduled Plans
Determines the number of scheduled jobs that ran in the past 30 days, how many were successful, how many ran but did not deliver or failed to run altogether.

#### Legacy Features
Outputs a list of legacy features that are still in use if any. These are features that have been replaced with improved ones and should be moved away from.

#### Version
Checks if the latest Looker version is being used. Looker supports only up to 3 releases back.

<a name="analyze_cmd"></a>
### Analyze Command
The `analyze` command is meant to help identify models and explores that have become bloated and use `vacuum` on them in order to trim them.

<a name="analyze_projects"></a>
#### analyze projects
The `analyze projects` command scans projects for their content as well as checks for the status of quintessential features for success such as the git connection status and validation requirements.
```
+-------------------+---------------+--------------+-------------------------+---------------------+-----------------------+
| project           |  model_count  |  view_count  | git_connection_status   | pull_request_mode   | validation_required   |
|-------------------+---------------+--------------+-------------------------+---------------------+-----------------------|
| marketing         |       1       |      13      | OK                      | links               | True                  |
| admin             |       2       |      74      | OK                      | off                 | True                  |
| powered_by_looker |       1       |      14      | OK                      | links               | True                  |
| salesforce        |       1       |      36      | OK                      | required            | False                 |
| thelook_event     |       1       |      17      | OK                      | required            | True                  |
+-------------------+---------------+--------------+-------------------------+---------------------+-----------------------+
```

<a name="analyze_models"></a>
#### analyze models
Shows the number of explores in each model as well as the number of queries against that model.
```
+-------------------+------------------+-----------------+-------------------+-------------------+
| project           | model            |  explore_count  |  unused_explores  |  query_run_count  |
|-------------------+------------------+-----------------+-------------------+-------------------|
| salesforce        | salesforce       |        8        |         0         |       39923       |
| thelook_event     | thelook          |       10        |         0         |      166307       |
| powered_by_looker | powered_by       |        5        |         0         |       49122       |
| marketing         | thelook_adwords  |        3        |         0         |       40869       |
| admin             | looker_base      |        0        |         0         |         0         |
| admin             | looker_on_looker |       10        |         9         |        28         |
+-------------------+------------------+-----------------+-------------------+-------------------+
```

<a name="analyze_explores"></a>
#### analyze explores
Shows explores and their usage. If the `--min_queries` argument is passed, joins and fields that have been used less than the threshold specified will be considered as unused.
```
+---------+-----------------------------------------+-------------+-------------------+--------------+----------------+---------------+-----------------+---------------+
| model   | explore                                 | is_hidden   | has_description   |  join_count  |  unused_joins  |  field_count  |  unused_fields  |  query_count  |
|---------+-----------------------------------------+-------------+-------------------+--------------+----------------+---------------+-----------------+---------------|
| thelook | cohorts                                 | True        | No                |      3       |       0        |      19       |        4        |      333      |
| thelook | data_tool                               | True        | No                |      3       |       0        |      111      |       90        |      736      |
| thelook | order_items                             | False       | No                |      7       |       0        |      153      |       16        |    126898     |
| thelook | events                                  | False       | No                |      6       |       0        |      167      |       68        |     19372     |
| thelook | sessions                                | False       | No                |      6       |       0        |      167      |       83        |     12205     |
| thelook | affinity                                | False       | No                |      2       |       0        |      34       |       13        |     3179      |
| thelook | orders_with_share_of_wallet_application | False       | No                |      9       |       0        |      161      |       140       |     1586      |
| thelook | journey_mapping                         | False       | No                |      11      |       2        |      238      |       228       |      14       |
| thelook | inventory_snapshot                      | False       | No                |      3       |       0        |      25       |       15        |      33       |
| thelook | kitten_order_items                      | True        | No                |      8       |       0        |      154      |       138       |      39       |
+---------+-----------------------------------------+-------------+-------------------+--------------+----------------+---------------+-----------------+---------------+
```

<a name="vacuum_cmd"></a>
### Vacuum Information
The `vacuum` command outputs a list of unused content based on predefined criteria that a developer can then use to cleanup models and explores.

<a name="vacuum_models"></a>
#### vacuum models
The `vacuum models` command exposes models and the number of queries against them over a predefined period of time. Explores that are listed here have not had the minimum number of queries against them in the timeframe specified. As a result it is safe to hide them and later delete them.
```
+------------------+---------------------------------------------+-------------------------+
| model            | unused_explores                             |  model_query_run_count  |
|------------------+---------------------------------------------+-------------------------|
| salesforce       | None                                        |          39450          |
| thelook          | None                                        |         164930          |
| powered_by       | None                                        |          49453          |
| thelook_adwords  | None                                        |          38108          |
| looker_on_looker | user_full                                   |           27            |
|                  | history_full                                |                         |
|                  | content_view                                |                         |
|                  | project_status                              |                         |
|                  | field_usage_full                            |                         |
|                  | dashboard_performance_full                  |                         |
|                  | user_weekly_app_activity_period_over_period |                         |
|                  | pdt_state                                   |                         |
|                  | user_daily_query_activity                   |                         |
+------------------+---------------------------------------------+-------------------------+
```

<a name="vacuum_explores"></a>
#### vacuum explores
The `vacuum explores` command exposes joins and exposes fields that are below the minimum number of queries threshold (default =0, can be changed using the `--min_queries` argument) over the specified timeframe (default: 90, can be changed using the `--timeframe` argument).

Example: from the analyze function run [above](#analyze_explores), we know that the cohorts explore has 4 fields that haven't been queried once in the past 90 days. Running the following vacuum command:

    $ henry vacuum explores --model thelook --explore cohorts

 provides the name of the unused fields:
```
+---------+-----------+----------------+------------------------------+
| model   | explore   | unused_joins   | unused_fields                |
|---------+-----------+----------------+------------------------------|
| thelook | cohorts   | users          | order_items.created_date     |
|         |           |                | order_items.id               |
|         |           |                | order_items.total_sale_price |
+---------+-----------+----------------+------------------------------+
```
If a join is unused, it's implying that fields introduced by that join haven't been used for the defined timeframe. For this reason fields exposed as a result of that join are not explicitly listed as unused fields.

It is very important to note that fields vacuumed fields in one explore are not meant to be completely removed from view files altogether because they might be used in other explores or joins. Instead, one should either hide those fields (if they're not used anywhere else) or exclude them from the explore using the _fields_ LookML parameter.

<a name="logging"></a>
## Logging
The tool logs activity as it's being used. Log files are stored in `~/.henry/log/` in your home directory. Sensitive information such as your client secret is filtered out for security reasons. Moreover, log files have restricted permissions which allow only the owner to read and write.

The logging module utilises a rotating file handler which is currently set to rollover when the current log file reaches 500 KB in size. The system saves old log files by adding the suffix '.1', '.2' etc., to the filename. The file being written to is always named `henry.log`. No more than 10 log files are kept at any point in time, ensuring logs do not consume more than 5 MB max.

<a name="dependencies"></a>
## Dependencies
- [PyYAML](https://pyyaml.org/): 3.12 or higher
- [requests](http://docs.python-requests.org/en/master/): 2.18.4 or higher
- [tabulate](https://bitbucket.org/astanin/python-tabulate): 0.8.2 or higher
- [tqdm](https://tqdm.github.io/): 4.23.4 or higher

<a name="development"></a>
## Development

To install henry in development mode you need to install the dependencies above and clone the project's repo with:

    $ git clone git@github.com:looker-open-source/henry.git

You can then install using:

    $ python setup.py develop

Alternatively, you can use `pip` if you want all the dependencies pulled in automatically (the -e option is for installing it in [development mode](https://pip.pypa.io/en/latest/reference/pip_install/#editable-installs)).

    $ pip install -e .

<a name="author"></a>
## Authors

Henry has primarily been developed by [Joseph Axisa](https://github.com/josephaxisa). See [all contributors](https://github.com/looker-open-source/henry/graphs/contributors).

<a name="contributing"></a>
## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/looker-open-source/henry/issues. This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [Contributor Covenant](http://contributor-covenant.org) code of conduct.

<a name="code_of_conduct"></a>
## Code of Conduct

Everyone interacting in the Henry project’s codebases, issue trackers, chat rooms and mailing lists is expected to follow the [code of conduct](https://github.com/looker-open-source/henry/blob/master/CODE_OF_CONDUCT.md).

<a name="copyright"></a>
## Copyright

Copyright (c) 2018 Joseph Axisa for Looker Data Sciences. See [MIT License](LICENSE.txt) for further details.
