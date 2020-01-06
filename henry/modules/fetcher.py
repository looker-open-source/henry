import abc
import csv
import datetime
import json
from operator import itemgetter
import re
from typing import cast, Dict, Optional, MutableSequence, Sequence, Union

import tabulate

from henry.modules import data_controller
from henry.modules import exceptions
from looker_sdk import client, methods, models


class Fetcher(abc.ABC):
    def __init__(self, user_input: data_controller.Input):
        self.sdk = self.configure_sdk(
            user_input.config_file, user_input.section, user_input.timeout
        )
        self.timeframe = f"{user_input.timeframe} days" if user_input else "90 days"
        self.min_queries = user_input.min_queries or 0
        self.limit = user_input.limit
        self.sortkey = user_input.sortkey
        cmd = user_input.command
        subcommand = user_input.subcommand or None
        self.full_cmd = f"{cmd}_{subcommand}" if subcommand else cmd
        self.save = user_input.save
        self.quiet = user_input.quiet

    def configure_sdk(
        self,
        config_file: Optional[str] = "looker.ini",
        section: Optional[str] = "looker",
        timeout: Optional[int] = None,
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
                    "history.query_run_count": f">{str(self.min_queries)}",
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
                explores.extend(
                    [self.sdk.lookml_model_explore(m.name, e.name) for e in m.explores]
                )
        return explores

    def get_used_explores(
        self, *, model: Optional[str] = None, explore: str = ""
    ) -> Dict[str, int]:
        """Returns a dictionary with used explore names as keys and query count as values"""
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
                },
                limit="5000",
            ),
        )
        _results: MutableSequence[Dict[str, int]] = json.loads(resp)
        results = {r["query.view"]: r["history.query_run_count"] for r in _results}
        return results

    def get_unused_explores(self, *, model):
        """Returns a list of explores that do not meet the min query count requirement
        for the specified timeframe.
        """
        _all = self.get_explores(model=model)
        used = self.get_used_explores(model=model)
        unused_explores = [e.name for e in _all if e.name not in used.keys()]
        return unused_explores

    def get_explore_fields(self, explore: models.LookmlModelExplore) -> Sequence[str]:
        """Return a list of non hidden fields for a given explore"""
        m = explore.model_name
        e = explore.name
        fields = explore.fields
        assert fields
        assert fields.dimensions and fields.measures  # and fields.filters
        dimensions = [f"{m}.{e}.{f.name}" for f in fields.dimensions if not f.hidden]
        measures = [f"{m}.{e}.{f.name}" for f in fields.measures if not f.hidden]
        # filters = [f"{m}.{e}.{f.name}" for f in fields.filters if not f.hidden]
        result = list(set([*dimensions, *measures]))  # *filters]))
        return result

    def get_used_explore_fields(
        self, *, model: str = "", explore: str = ""
    ) -> Dict[str, int]:
        """Returns a list of model.view scoped explore fields as well as the
        number of times they were used in the specified timeframe as value.
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
                    "query.model": model.replace("_", "^_") if model else "",
                    "query.view": explore.replace("_", "^_") if explore else "",
                    "query.formatted_fields": "-NULL",
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
        all_joins = explore.scopes.remove(explore.name)
        explore_field_stats = {j.split(".")[0]: qc for j, qc in field_stats}

        join_stats: Dict[str, int] = {}
        for join, query_count in explore_field_stats.items():
            if join_stats.get(join):
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

    def _filter(self, data: Dict[str, int]) -> Dict[str, int]:
        """Filters on min_queries, this happens after querying the db"""
        return dict(filter(lambda e: e[1] > self.min_queries), data.items())

    def _limit(self, data):
        """Limits results printed on screen"""
        data = data[: self.limit] if self.limit else data
        return data

    def _sort(self, data: TData) -> TData:
        """Sorts results as specified by user"""
        if self.sortkey:
            sort_key = self.sortkey[0]
            if sort_key not in data[0].keys():
                raise KeyError(f"Sort field {sort_key} not found.")

            sort_types = {"ASC": False, "DESC": True}
            if self.sortkey[1].upper() not in sort_types.keys():
                raise KeyError(f"Unrecognized sort type: {sortkey[1]}.")
            sort_type = sort_types[self.sortkey[1].upper()]

            data = sorted(data, key=itemgetter(sort_key), reverse_type=sort_type)
        return data

    def _save_to_file(self, data: Sequence[Dict[str, Union[int, str]]]):
        """Save results to a file with name {command}_date_time.csv"""
        date = datetime.datetime.now().strftime("%y%m%d_%H%M%S")
        filename = f"{self.full_command}_{date}.csv"
        with open(filename, "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)

    def _tabularize_and_print(
        self, data: Sequence[Dict[str, Union[int, str, bool]]],
    ):
        """Prints data in tabular form."""
        if not data:
            print("No results found.", end="\n" * 2)
        else:
            result = tabulate.tabulate(
                data, headers="keys", tablefmt="psql", numalign="center"
            )
            print(f"\b{result}", end="\n" * 2)

    def output(self, data: Sequence[Dict[str, Union[int, str, bool]]]):
        """Output generated results and/or save"""
        data = self._limit(data)
        data = self._sort(data)
        if self.save:
            self._save_to_file(data)
        if not self.quiet:
            self._tabularize_and_print(data)
