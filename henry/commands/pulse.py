import json
import logging
from textwrap import fill
from typing import cast, Dict, MutableSequence, Optional, Union, Sequence

from looker_sdk import models

from henry.modules import data_controller
from henry.modules import exceptions
from henry.modules import fetcher
from henry.modules import spinner


class Pulse(fetcher.Fetcher):
    """Runs a number of checks against a given Looker instance to determine
    overall health.
    """

    @classmethod
    def run(cls, output_options: Optional[Dict[str, bool]] = None):
        pulse = cls(output_options=output_options)
        pulse.check_db_connections()
        pulse.check_dashboard_performance()
        pulse.check_dashboard_errors()
        pulse.check_explore_performance()
        pulse.check_schedule_failures()
        pulse.check_legacy_features()

    @spinner.Spinner()
    def check_db_connections(self) -> Sequence[Dict[str, str]]:
        """Gets all db connections and runs all supported tests against them.
        """
        print("\bTest 1/6: Checking connections")
        reserved_names = ["looker__internal__analytics", "looker"]
        db_connections: Sequence[models.DBConnection] = list(
            filter(
                lambda c: c.name not in reserved_names,
                self.sdk.all_connections(),  # noqa: E501
            )
        )

        if not db_connections:
            raise exceptions.NotFoundError("No connections found.")

        formatted_results: MutableSequence[Dict[str, str]] = []
        for connection in db_connections:
            assert connection.dialect
            assert isinstance(connection.name, str)
            results = self.sdk.test_connection(
                cast(str, connection.name),
                models.DelimSequence(connection.dialect.connection_tests),
            )
            results = list(filter(lambda r: r.status == "error", results))
            errors = [
                f"- {fill(cast(str, e.message), width=100)}" for e in results
            ]  # noqa: E501
            formatted_results.append(
                {
                    "Connection": connection.name,
                    "Status": "OK" if not errors else "\n".join(errors),
                }
            )
        data_controller.tabularize_and_print(formatted_results)

    @spinner.Spinner()
    def check_dashboard_performance(self):
        """Prints a list of dashboards with slow running queries in the past
        7 days"""
        print(
            "\bTest 2/6: Checking for dashboards with queries slower than "
            "30 seconds in the last 7 days"
        )
        request = models.WriteQuery(
            model="system__activity",
            view="scheduled_plan",
            fields=["dashboard.title, query.count"],
            filters={
                "history.created_date": "7 days",
                "history.is_single_query": "Yes",
                "history.real_dash_id": "-NULL",
                "history.runtime": ">30",
                "history.status": "complete",
            },
            sorts=["query.count desc"],
            limit=20,
        )

        result = self.sdk.run_inline_query("json", request)
        slowest_dashboards = json.loads(result)
        data_controller.tabularize_and_print(slowest_dashboards)

    @spinner.Spinner()
    def check_dashboard_errors(self):
        """Prints a list of erroring dashboard queries."""
        print(
            "\bTest 3/6: Checking for dashboards with erroring queries in "
            "the last 7 days"
        )
        request = models.WriteQuery(
            model="system__activity",
            view="history",
            fields=["dashboard.title", "history.query_run_count"],
            filters={
                "history.created_date": "7 days",
                "history.dashboard_session": "-NULL",
                "history.is_single_query": "Yes",
                "history.status": "error",
            },
            sorts=["history.query_run_ount desc"],
            limit=20,
        )
        result = self.sdk.run_inline_query("json", request)
        erroring_dashboards = json.loads(result)
        data_controller.tabularize_and_print(erroring_dashboards)

    @spinner.Spinner()
    def check_explore_performance(self):
        """ a list of the slowest running explores."""
        print("\bTest 4/6: Checking for the slowest explores in the past 7 days")
        request = models.WriteQuery(
            model="system__activity",
            view="history",
            fields=["query.model", "query.view", "history.average_runtime"],
            filters={
                "history.created_date": "7 days",
                "query.model": "-NULL, -system^_^_activity",
            },
            sorts=["history.average_runtime desc"],
            limit=20,
        )
        slowest_explores_json = self.sdk.run_inline_query("json", request)
        slowest_explores = json.loads(slowest_explores_json)

        request.fields = ["history.average_run_time"]
        avg_query_runtime = self.sdk.run_inline_query("json", request)

        data_controller.tabularize_and_print(slowest_explores)

    @spinner.Spinner()
    def check_schedule_failures(self):
        """Prints a list of schedules that have failed in the past 7 days."""
        print("\bTest 5/6: Checking for failing schedules")
        request = models.WriteQuery(
            model="system__activity",
            view="scheduled_plan",
            fields=["scheduled_job.name", "scheduled_job.count"],
            filters={
                "scheduled_job.created_date": "7 days",
                "scheduled_job.status": "failure",
            },
            sorts=["scheduled_job.count desc"],
            limit=500,
        )
        result = self.sdk.run_inline_query("json", request)
        failed_schedules = json.loads(result)
        data_controller.tabularize_and_print(failed_schedules)

    @spinner.Spinner()
    def check_legacy_features(self):
        """Prints a list of enabled legacy features."""
        print("\bTest 6/6: Checking for enabled legacy features")
        lf = list(filter(lambda f: f.enabled, self.sdk.all_legacy_features()))
        legacy_features = [{"Feature": cast(str, f.name)} for f in lf]
        data_controller.tabularize_and_print(legacy_features)
