![image](https://github.com/looker-open-source/henry/blob/master/doc/logo/logo.png?raw=true)

---

# Henry: A Looker Cleanup Tool

Henry is a command line tool that helps determine model bloat in your Looker instance and identify unused content in models and explores. It is meant to help developers cleanup models from unused explores and explores from unused joins and fields, as well as maintain a healthy and user-friendly instance.

## Table of Contents

- [Henry: A Looker Cleanup Tool](#henry-a-looker-cleanup-tool)
  - [Table of Contents](#table-of-contents)
  - [Status and Support](#status-and-support)
  - [Installation](#installation)
  - [Usage](#usage)
    - [Global Options that apply to many commands](#global-options-that-apply-to-many-commands)
      - [API timeout settings](#api-timeout-settings)
      - [Output to File](#output-to-file)
    - [Pulse Command](#pulse-command)
    - [Analyze Command](#analyze-command)
      - [analyze projects](#analyze-projects)
      - [analyze models](#analyze-models)
      - [analyze explores](#analyze-explores)
    - [Vacuum Information](#vacuum-information)
      - [vacuum models](#vacuum-models)
      - [vacuum explores](#vacuum-explores)
  - [Contributing](#contributing)
  - [Code of Conduct](#code-of-conduct)
  - [Copyright](#copyright)

<a name="status_and_support"></a>

## Status and Support

Henry is **NOT** supported or warranted by Looker in any way. Please do not contact Looker support
for issues with Henry. Issues can be logged via https://github.com/looker-open-source/henry/issues

<a name="where_to_get_it"></a>

## Installation

Henry requires python3.7+. It is published on [PyPI](https://pypi.org/project/henry/) and can be installed using pip:

    $ pip install henry

For development setup, follow the Development setup [below](#development).

<a name="usage"></a>

## Usage

In order to display usage information, use:

    $ henry --help

<a name="global_options"></a>

### Global Options that apply to many commands

<a name="authentication"></a>

#### Authentication

Henry makes use of the [Looker SDK](https://github.com/looker-open-source/sdk-codegen/tree/master/python) to issue API calls and requires API3 credentials. These can provided either using an .ini file or environment variables as documented [here](https://github.com/looker-open-source/sdk-codegen#environment-variable-configuration). By default, the tool looks for a "looker.ini" file in the working directory. If the configuration file is named differently or located elsewhere, it must be specified using the `--config-file` argument.

Example .ini file:

```
[Looker]
# Base URL for API. Do not include /api/* in the url
base_url=https://self-signed.looker.com:19999
# API 3 client id
client_id=YourClientID
# API 3 client secret
client_secret=YourClientSecret
# Set to false if testing locally against self-signed certs. Otherwise leave True
verify_ssl=True

[Production]
base_url=https://production.looker.com:19999
client_id=YourClientID
client_secret=YourClientSecret
verify_ssl=True
```

Assuming the above ini file contents, Henry can be run as follows:

    $ henry pulse --config-file=looker.ini --section=Looker

which due to defaults, is equivalent to

    $ henry pulse

Running it using the details under the `Production` section can be done as follows:

    $ henry pulse --section=Production

<a name="api_timeout_settings"></a>

#### API timeout settings

By default, API calls have a timeout of 120 seconds. This can be overriden using the `--timeout` argument.

<a name="output_to_file"></a>

#### Output to File

If the `--save` flag is used the tool saves the results to your current working directory. Example usage:

    $ henry vacuum models --save

saves the results in _vacuum_models\_{date}\_{time}.csv_ in the current working directory.

<a name="pulse_cmd"></a>

### Pulse Command

The command `henry pulse` runs a number of tests that help determine the overall instance health.

<a name="analyze_cmd"></a>

### Analyze Command

The `analyze` command is meant to help identify models and explores that have become bloated and use `vacuum` on them in order to trim them.

<a name="analyze_projects"></a>

#### analyze projects

The `analyze projects` command scans projects for their content as well as checks for the status of quintessential features for success such as the git connection status and validation requirements.

```
+-------------------+---------------+--------------+-------------------------+---------------------+------------------------+
| Project           |  # Models     | # View Files | Git Connection Status   | PR Mode             | Is Validation Required |
|-------------------+---------------+--------------+-------------------------+---------------------+------------------------|
| marketing         |       1       |      13      | OK                      | links               | True                   |
| admin             |       2       |      74      | OK                      | off                 | True                   |
| powered_by_looker |       1       |      14      | OK                      | links               | True                   |
| salesforce        |       1       |      36      | OK                      | required            | False                  |
| thelook_event     |       1       |      17      | OK                      | required            | True                   |
+-------------------+---------------+--------------+-------------------------+---------------------+------------------------+
```

<a name="analyze_models"></a>

#### analyze models

Shows the number of explores in each model as well as the number of queries against that model.

```
+-------------------+------------------+-----------------+-------------------+-------------------+
| Project           | Model            |  # Explores     | # Unused Explores |    Query Count    |
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

Shows explores and their usage. If the `--min-queries` argument is passed, joins and fields that have been used less than the threshold specified will be considered as unused.

```
+---------+-----------------------------------------+-------------+-------------------+--------------+----------------+---------------+-----------------+---------------+
| Model   | Explore                                 | Is Hidden   | Has Description   |   # Joins    | # Unused Joins |    # Fields   | # Unused Fields |  Query Count  |
|---------+-----------------------------------------+-------------+-------------------+--------------+----------------+---------------+-----------------+---------------|
| thelook | cohorts                                 | True        | False             |      3       |       0        |      19       |        4        |      333      |
| thelook | data_tool                               | True        | False             |      3       |       0        |      111      |       90        |      736      |
| thelook | order_items                             | False       | True              |      7       |       0        |      153      |       16        |    126898     |
| thelook | events                                  | False       | True              |      6       |       0        |      167      |       68        |     19372     |
| thelook | sessions                                | False       | False             |      6       |       0        |      167      |       83        |     12205     |
| thelook | affinity                                | False       | False             |      2       |       0        |      34       |       13        |     3179      |
| thelook | orders_with_share_of_wallet_application | False       | True              |      9       |       0        |      161      |       140       |     1586      |
| thelook | journey_mapping                         | False       | False             |      11      |       2        |      238      |       228       |      14       |
| thelook | inventory_snapshot                      | False       | False             |      3       |       0        |      25       |       15        |      33       |
| thelook | kitten_order_items                      | True        | False             |      8       |       0        |      154      |       138       |      39       |
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
| Model            | Explore                                     |     Model Query Count   |
|------------------+---------------------------------------------+-------------------------|
| salesforce       |                                             |          39450          |
| thelook          |                                             |         164930          |
| powered_by       |                                             |          49453          |
| thelook_adwords  |                                             |          38108          |
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

The `vacuum explores` command exposes joins and exposes fields that are below or equal to the minimum number of queries threshold (default=0, can be changed using the `--min-queries` argument) over the specified timeframe (default: 90, can be changed using the `--timeframe` argument).

Example: from the analyze function run [above](#analyze_explores), we know that the cohorts explore has 4 fields that haven't been queried once in the past 90 days. Running the following vacuum command:

    $ henry vacuum explores --model thelook --explore cohorts

provides the name of the unused fields:

```
+---------+-----------+----------------+------------------------------+
| Model   | Explore   | Unused Joins   | Unused Fields                |
|---------+-----------+----------------+------------------------------|
| thelook | cohorts   | users          | users.id                     |
|         |           |                | order_items.id               |
|         |           |                | order_items.id               |
|         |           |                | order_items.total_sale_price |
+---------+-----------+----------------+------------------------------+
```

If a join is unused, it's implying that fields introduced by that join haven't been used for the defined timeframe. For this reason fields exposed as a result of that join are not explicitly listed as unused fields.

It is very important to note that fields listed as unused in one explore are not meant to be completely removed from view files altogether because they might be used in other explores (via extensions), or filters. Instead, one should either hide those fields (if they're not used anywhere else) or exclude them from the explore using the _fields_ LookML parameter.

<a name="contributing"></a>

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/looker-open-source/henry/issues. This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [Contributor Covenant](http://contributor-covenant.org) code of conduct.

<a name="code_of_conduct"></a>

## Code of Conduct

Everyone interacting in the Henry project’s codebases, issue trackers, chat rooms and mailing lists is expected to follow the [code of conduct](https://github.com/looker-open-source/henry/blob/master/CODE_OF_CONDUCT.md).

<a name="copyright"></a>

## Copyright

Copyright (c) 2018 Joseph Axisa for Looker Data Sciences. See [MIT License](LICENSE.txt) for further details.
