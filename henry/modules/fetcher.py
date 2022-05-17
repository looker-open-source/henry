import csv
import datetime
import json
import re
import uuid
from operator import itemgetter
from typing import (
    Callable,
    Dict,
    MutableSequence,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)

import tabulate
from looker_sdk import error
from looker_sdk.rtl import api_settings, auth_session, requests_transport, serialize
from looker_sdk.sdk.api40 import methods, models

from henry.modules import exceptions

from .. import __version__ as pkg

TResult = MutableSequence[Dict[str, Union[str, int, bool]]]


class Fetcher:
    def __init__(self, options: "Input"):
        self.timeframe = f"{options.timeframe} days" if options.timeframe else "90 days"
        self.min_queries = options.min_queries or 0
        self.limit = options.limit[0] if options.limit else None
        self.sortkey = options.sortkey
        cmd = options.command
        sub_cmd = options.subcommand or None
        self.cmd = f"{cmd}_{sub_cmd}" if sub_cmd else cmd
        self.save = options.save
        self.quiet = options.quiet
        self.sdk = self.configure_sdk(
            options.config_file, options.section, options.timeout
        )
        self._verify_api_credentials()

    def configure_sdk(
        self,
        config_file: str,
        section: str,
        timeout: Optional[int],
    ) -> methods.Looker40SDK:
        """Instantiates and returns a LookerSDK object and overrides default timeout if
        specified by user.
        """
        settings = api_settings.ApiSettings(filename=config_file, section=section)
        user_agent_tag = f"Henry v{pkg.__version__}: cmd={self.cmd}, sid={uuid.uuid1()}"
        settings.headers = {
            "Content-Type": "application/json",
            "User-Agent": user_agent_tag,
        }
        if timeout:
            settings.timeout = timeout
        transport = requests_transport.RequestsTransport.configure(settings)
        # 4.0 is hardcoded here due to needing the -40 suffixed methods
        return methods.Looker40SDK(
            auth_session.AuthSession(
                settings, transport, serialize.deserialize40, "4.0"
            ),
            serialize.deserialize40,
            serialize.serialize40,
            transport,
            "4.0",
        )

    def _verify_api_credentials(self):
        try:
            self.sdk.me()
        except error.SDKError as e:
            print("Error retreiving self using API. Please check your credentials.")
            raise (e)

    def get_projects(
        self, project_id: Optional[str] = None
    ) -> Sequence[models.Project]:
        """Returns a list of projects."""
        try:
            if project_id:
                projects: Sequence[models.Project] = [self.sdk.project(project_id)]
            else:
                projects = self.sdk.all_projects()
        except error.SDKError:
            raise exceptions.NotFoundError("An error occured while getting projects.")
        return projects

    def get_models(
        self, *, project: Optional[str] = None, model: Optional[str] = None
    ) -> Sequence[models.LookmlModel]:
        """Returns a list of lookml models."""
        ret: Sequence[models.LookmlModel]
        if project:
            self.get_projects(project)
        try:
            if model:
                ml: Sequence[models.LookmlModel] = [self.sdk.lookml_model(model)]
            else:
                ml = self.sdk.all_lookml_models()
        except error.SDKError:
            raise exceptions.NotFoundError("An error occured while getting models.")
        else:
            if project:
                # .lower() is used so behavior is consistent with /project endpoint
                ml = list(
                    filter(
                        lambda m: m.project_name.lower() == project.lower(),
                        ml,
                    )  # type: ignore  # noqa: B950
                )
            ml = list(filter(lambda m: cast(bool, m.has_content), ml))
        return ml

    def get_used_models(self) -> Dict[str, int]:
        """Returns a dictionary with model names as keys and query count as values."""
        resp = self.sdk.run_inline_query(
            "json",
            models.WriteQuery(
                model="i__looker",
                view="history",
                fields=["history.query_run_count, query.model"],
                filters={
                    "history.created_date": self.timeframe,
                    "query.model": "-system^_^_activity, -i^_^_looker",
                    "history.query_run_count": ">0",
                    "user.dev_mode": "No",
                },
                limit="5000",
            ),
        )
        _results: MutableSequence[Dict[str, int]] = json.loads(resp)
        results = {
            str(row["query.model"]): int(row["history.query_run_count"])
            for row in _results
        }
        return results

    def get_explores(
        self, *, model: Optional[str] = None, explore: Optional[str] = None
    ) -> Sequence[models.LookmlModelExplore]:
        """Returns a list of explores."""
        try:
            if model and explore:
                explores = [self.sdk.lookml_model_explore(model, explore)]
            elif not explore:
                all_models = self.get_models(model=model)
                explores = []
                for m in all_models:
                    assert isinstance(m.name, str)
                    assert isinstance(m.explores, list)
                    explores.extend(
                        [
                            self.sdk.lookml_model_explore(m.name, cast(str, e.name))
                            for e in m.explores
                        ]
                    )
        except error.SDKError:
            raise exceptions.NotFoundError(
                "An error occured while getting models/explores."
            )
        return explores

    def get_used_explores(
        self, *, model: Optional[str] = None, explore: str = ""
    ) -> Dict[str, int]:
        """Returns a dictionary with used explore names as keys and query count as
        values.
        """
        resp = self.sdk.run_inline_query(
            "json",
            models.WriteQuery(
                model="i__looker",
                view="history",
                fields=["query.view", "history.query_run_count"],
                filters={
                    "history.created_date": self.timeframe,
                    "query.model": model.replace("_", "^_") if model else "",
                    "history.query_run_count": ">0",
                    "query.view": explore,
                    "user.dev_mode": "No",
                },
                limit="5000",
            ),
        )
        _results: MutableSequence[Dict[str, int]] = json.loads(resp)
        results = {
            cast(str, r["query.view"]): r["history.query_run_count"] for r in _results
        }
        return results

    def get_unused_explores(self, model: str):
        """Returns a list of explores that do not meet the min query count requirement
        for the specified timeframe.
        """
        _all = self.get_explores(model=model)
        used = self.get_used_explores(model=model)
        # Keep only explores that satisfy the min_query requirement
        used = self._filter(data=used, condition=lambda x: x[1] >= self.min_queries)
        unused_explores = [e.name for e in _all if e.name not in used.keys()]
        return unused_explores

    def get_explore_fields(self, explore: models.LookmlModelExplore) -> Sequence[str]:
        """Return a list of non hidden fields for a given explore"""
        fields = explore.fields
        dimensions = [cast(str, f.name) for f in fields.dimensions if not f.hidden]  # type: ignore  # noqa: B950
        measures = [cast(str, f.name) for f in fields.measures if not f.hidden]  # type: ignore  # noqa B950
        result = sorted(list(set([*dimensions, *measures])))
        return result

    def get_used_explore_fields(
        self, *, model: str, explore: str = ""
    ) -> Dict[str, int]:
        """Returns a list of model.view scoped explore fields as well as the
        number of times they were used in the specified timeframe as value.
        Should always be called with either model, or model and explore
        """
        resp = self.sdk.run_inline_query(
            "json",
            models.WriteQuery(
                model="i__looker",
                view="history",
                fields=[
                    "query.model",
                    "query.view",
                    "query.formatted_fields",
                    "query.formatted_filters",
                    "history.query_run_count",
                ],
                filters={
                    "history.created_date": self.timeframe,
                    "query.model": model.replace("_", "^_"),
                    "query.view": explore.replace("_", "^_") if explore else "",
                    "query.formatted_fields": "-NULL",
                    "history.workspace_id": "production",
                },
                limit="5000",
            ),
        )
        data = json.loads(resp)
        used_fields: Dict[str, int] = {}
        for row in data:
            model = row["query.model"]
            explore = row["query.view"]
            fields = re.findall(r"(\w+\.\w+)", row["query.formatted_fields"])
            recorded = []
            for f in fields:
                if used_fields.get(f):
                    used_fields[f] += row["history.query_run_count"]
                else:
                    used_fields[f] = row["history.query_run_count"]
                recorded.append(f)

            # A field used as a filter in a query is not listed in
            # query.formatted_fields BUT if the field is used as both a filter
            # and a dimension/measure, it's listed in both query.formatted_fields
            # and query.formatted_filters. The recorded variable keeps track of
            # this, so that no double counting occurs.
            filters = row["query.formatted_filters"]
            if filters:
                parsed_filters = re.findall(r"(\w+\.\w+)+", filters)
                for f in parsed_filters:
                    if f in recorded:
                        continue
                    elif used_fields.get(f):
                        used_fields[f] += row["history.query_run_count"]
                    else:
                        used_fields[f] = row["history.query_run_count"]
        return used_fields

    def get_explore_field_stats(
        self, explore: models.LookmlModelExplore
    ) -> Dict[str, int]:
        """Return a dictionary with all exposed field names as keys and field query
        count as values.
        """
        assert isinstance(explore.model_name, str)
        assert isinstance(explore.name, str)
        all_fields = self.get_explore_fields(explore=explore)
        field_stats = self.get_used_explore_fields(
            model=explore.model_name, explore=explore.name
        )

        for field in all_fields:
            if not field_stats.get(field):
                field_stats[field] = 0

        return field_stats

    def get_explore_join_stats(
        self, *, explore: models.LookmlModelExplore, field_stats: Dict[str, int]
    ) -> Dict[str, int]:
        """Returns dict containing stats about all joins in an explore."""
        assert isinstance(explore.scopes, MutableSequence)
        all_joins = explore.scopes
        all_joins.remove(explore.name)
        join_stats: Dict[str, int] = {}
        if all_joins:
            for field, query_count in field_stats.items():
                join = field.split(".")[0]  # Because all fields are view (join) scoped
                if join == explore.name:
                    continue
                elif join_stats.get(join):
                    join_stats[join] += query_count
                else:
                    join_stats[join] = query_count

            for join in all_joins:
                if not join_stats.get(join):
                    join_stats[join] = 0
        return join_stats

    def run_git_connection_tests(self, project_id: str):
        """Run all git connection tests for a given project."""
        self.sdk.update_session(models.WriteApiSession(workspace_id="dev"))
        supported_tests = self.sdk.all_git_connection_tests(project_id)
        results = []
        for test in supported_tests:
            assert isinstance(test.id, str)
            resp = self.sdk.run_git_connection_test(project_id, test.id)
            results.append(resp)
            if resp.status != "pass":
                break
        self.sdk.update_session(models.WriteApiSession(workspace_id="production"))
        errors = list(filter(lambda r: r.status != "pass", results))
        formatted_results = [f"{r.id} ({r.status})" for r in results]
        return "\n".join(formatted_results) if errors else "OK"

    def _filter(
        self, data: Optional[Dict[str, int]], condition: Optional[Callable] = None
    ) -> Dict[str, int]:
        """Filters based on min_queries condition. By default, it returns rows that do
        not satisfy the min_queries requirement. "condition" can be passed to override
        this behavior.
        """
        result: Dict[str, int]
        if not data:
            result = dict()
        elif condition:
            result = dict(filter(condition, data.items()))
        else:
            result = dict(filter(lambda e: e[1] <= self.min_queries, data.items()))
        return result

    def _limit(
        self, data: Sequence[Dict[str, Union[int, str, bool]]]
    ) -> Sequence[Dict[str, Union[int, str, bool]]]:
        """Limits results printed on screen"""
        data = data[: self.limit] if self.limit else data
        return data

    def _sort(
        self, data: Sequence[Dict[str, Union[int, str, bool]]]
    ) -> Sequence[Dict[str, Union[int, str, bool]]]:
        """Sorts results as specified by user"""
        if self.sortkey:
            sort_key = self.sortkey[0]
            if sort_key not in data[0].keys():
                raise KeyError(f"Sort field {sort_key} not found.")

            sort_types = {"ASC": False, "DESC": True}
            if self.sortkey[1].upper() not in sort_types.keys():
                raise KeyError(f"Unrecognized sort type: {self.sortkey[1]}.")
            sort_type = sort_types[self.sortkey[1].upper()]

            data = sorted(data, key=itemgetter(sort_key), reverse=sort_type)
        return data

    def _save_to_file(self, data: Sequence[Dict[str, Union[int, str]]]):
        """Save results to a file with name {command}_date_time.csv"""
        date = datetime.datetime.now().strftime("%y%m%d_%H%M%S")
        filename = f"{self.cmd}_{date}.csv"
        with open(filename, "w", newline="") as csvfile:
            # Replace "\n" which is required when printing, with ','
            data = list(
                map(
                    lambda x: {k: str(v).replace("\n", ",") for k, v in x.items()},
                    data,
                )
            )
            writer = csv.DictWriter(csvfile, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)

    def _tabularize_and_print(
        self,
        data: Sequence[Dict[str, Union[int, str, bool]]],
    ):
        """Prints data in tabular form."""
        if not data:
            print("\bNo results found.", end="\n" * 2)
        else:
            result = tabulate.tabulate(
                data, headers="keys", tablefmt="psql", numalign="center"
            )
            print(f"\b{result}", end="\n" * 2)

    def output(self, data: Sequence[Dict[str, Union[int, str, bool]]]):
        """Output generated results and/or save"""
        data = self._sort(data)
        data = self._limit(data)
        if self.save:
            self._save_to_file(data)
        if not self.quiet:
            self._tabularize_and_print(data)


class Input(NamedTuple):
    command: str
    subcommand: Optional[str] = None
    project: Optional[str] = None
    model: Optional[str] = None
    explore: Optional[str] = None
    timeframe: Optional[int] = 90
    min_queries: Optional[int] = 0
    sortkey: Optional[Tuple[str, str]] = None
    limit: Optional[Sequence[int]] = None
    config_file: str = "looker.ini"
    section: str = "Looker"
    quiet: bool = False
    save: Optional[bool] = False
    timeout: Optional[int] = 120
