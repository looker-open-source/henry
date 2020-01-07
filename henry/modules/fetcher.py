import abc
import csv
import datetime
import json
from operator import itemgetter
import re
from typing import (
    Callable,
    cast,
    Dict,
    Optional,
    NamedTuple,
    MutableSequence,
    Union,
    Sequence,
    Tuple,
)

import tabulate
from looker_sdk import client, methods, models

from henry.modules import exceptions


TResult = MutableSequence[Dict[str, Union[str, int, bool]]]


class Fetcher(abc.ABC):
    def __init__(self, options: "Input"):
        self.sdk = self.configure_sdk(
            options.config_file, options.section, options.timeout
        )
        self.timeframe = f"{options.timeframe} days" if options.timeframe else "90 days"
        self.min_queries = options.min_queries or 0
        self.limit = options.limit[0] if options.limit else None
        self.sortkey = options.sortkey
        cmd = options.command
        subcommand = options.subcommand or None
        self.full_command = f"{cmd}_{subcommand}" if subcommand else cmd
        self.save = options.save
        self.quiet = options.quiet

    def configure_sdk(
        self, config_file: str, section: str, timeout: Optional[int] = 120,
    ) -> methods.LookerSDK:
        sdk = client.setup(config_file, section)
        sdk.transport
        if timeout:
            sdk.transport.settings.timeout = timeout
        return sdk

    def get_models(
        self, *, project: Optional[str] = None, model: Optional[str] = None
    ) -> Sequence[models.LookmlModel]:
        """Returns a list of lookml models."""
        ml: Sequence[models.LookmlModel]
        if model:
            ml = [self.sdk.lookml_model(model)]
        else:
            ml = self.sdk.all_lookml_models()

        if project:
            ml = list(filter(lambda m: m.project_name == project, ml))
        ml = list(filter(lambda m: m.has_content, ml))

        if not ml:
            raise exceptions.NotFoundError("No populated model files found.")

        return ml

    def get_used_models(self) -> Dict[str, int]:
        """Returns a dictionary with model names as keys and query count as values."""
        # TODO: filter on min_queries
        resp = self.sdk.run_inline_query(
            "json",
            models.WriteQuery(
                model="system__activity",
                view="history",
                fields=["history.query_run_count, query.model"],
                filters={
                    "history.created_date": self.timeframe,
                    "query.model": "-system^_^_activity",
                    "history.query_run_count": ">0",
                    "history.workspace_id": "production",
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
        if model and explore:
            explores = [self.sdk.lookml_model_explore(model, explore)]
        else:
            all_models = self.get_models(model=model)
            explores = []
            for m in all_models:
                assert isinstance(m.name, str)
                assert m.explores
                explores.extend(
                    [
                        self.sdk.lookml_model_explore(m.name, cast(str, e.name))
                        for e in m.explores
                    ]
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
                model="system__activity",
                view="history",
                fields=["query.view", "history.query_run_count"],
                filters={
                    "history.created_date": self.timeframe,
                    "query.model": model.replace("_", "^_") if model else "",
                    "history.query_run_count": ">0",
                    "query.view": explore,
                    "history.workspace_id": "production",
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
        assert fields
        assert fields.dimensions and fields.measures  # and fields.filters
        dimensions = [cast(str, f.name) for f in fields.dimensions if not f.hidden]
        measures = [cast(str, f.name) for f in fields.measures if not f.hidden]
        # filters = [f"{m}.{e}.{f.name}" for f in fields.filters if not f.hidden]
        result = list(set([*dimensions, *measures]))  # *filters]))
        return result

    def get_used_explore_fields(
        self, *, model: str, explore: str = ""
    ) -> Dict[str, int]:
        """Returns a list of model.view scoped explore fields as well as the
        number of times they were used in the specified timeframe as value.
        Should always be called with either model, or model and explore
        """
        # WARNING: fields used in filters are not found in query.formatted_fields
        resp = self.sdk.run_inline_query(
            "json",
            models.WriteQuery(
                model="system__activity",
                view="history",
                fields=[
                    "query.model",
                    "query.view",
                    "query.formatted_fields",
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
            for f in fields:
                if used_fields.get(f):
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
        all_fields = self.get_explore_fields(explore=explore)
        field_stats = self.get_used_explore_fields(
            model=cast(str, explore.model_name), explore=cast(str, explore.name)
        )

        for field in all_fields:
            if not field_stats.get(field):
                field_stats[field] = 0

        return field_stats

    def get_explore_join_stats(
        self, *, explore: models.LookmlModelExplore, field_stats: Dict[str, int]
    ) -> Dict[str, int]:
        """Returns dict containing stats about all joins in an explore."""
        assert explore.scopes
        all_joins = cast(MutableSequence, explore.scopes)
        all_joins.remove(explore.name)
        join_stats: Dict[str, int] = {}
        if all_joins:
            for field, query_count in field_stats.items():
                join = field.split(".")[0]  # Because all fields are view scoped
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
            resp = self.sdk.run_git_connection_test(
                project_id, cast(str, test.id)
            )  # noqa: E501
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

    def _limit(self, data):
        """Limits results printed on screen"""
        data = data[: self.limit] if self.limit else data
        return data

    def _sort(self, data):
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
        filename = f"{self.full_command}_{date}.csv"
        with open(filename, "w", newline="") as csvfile:
            # Replace "\n" which is required when printing, with ','
            data = list(
                map(
                    lambda x: {
                        k: cast(str, v).replace("\n", ",") for k, v in x.items()
                    },
                    data,
                )
            )
            writer = csv.DictWriter(csvfile, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)

    def _tabularize_and_print(
        self, data: Sequence[Dict[str, Union[int, str, bool]]],
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
    section: str = "looker"
    quiet: bool = False
    save: Optional[bool] = False
    timeout: Optional[int] = 120
