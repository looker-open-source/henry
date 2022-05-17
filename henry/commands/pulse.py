import json
from textwrap import fill
from typing import Sequence, cast

from looker_sdk import models
from looker_sdk.error import SDKError

from henry.modules import exceptions, fetcher, spinner


class Pulse(fetcher.Fetcher):
    """Runs a number of checks against a given Looker instance to determine
    overall health.
    """

    @classmethod
    def run(cls, user_input: fetcher.Input):
        pulse = cls(user_input)
        pulse.check_db_connections()
        pulse.check_dashboard_performance()
        pulse.check_dashboard_errors()
        pulse.check_explore_performance()
        pulse.check_schedule_failures()
        pulse.check_legacy_features()

    @spinner.Spinner()
    def check_db_connections(self):
        """Gets all db connections and runs all supported tests against them."""
        print("\bTest 1/6: Checking connections")

        reserved_names = ["looker__internal__analytics", "looker", "looker__ilooker"]
        db_connections: Sequence[models.DBConnection] = list(
            filter(lambda c: c.name not in reserved_names, self.sdk.all_connections())
        )

        if not db_connections:
            raise exceptions.NotFoundError("No connections found.")

        formatted_results = []
        for connection in db_connections:
            assert connection.dialect
            assert isinstance(connection.name, str)
            resp = self.sdk.test_connection(
                connection.name,
                models.DelimSequence(connection.dialect.connection_tests),
            )
            results = list(filter(lambda r: r.status == "error", resp))
            errors = [f"- {fill(cast(str, e.message), width=100)}" for e in results]
            resp = self.sdk.run_inline_query(
                "json",
                models.WriteQuery(
                    model="i__looker",
                    view="history",
                    fields=["history.query_run_count"],
                    filters={"history.connection_name": connection.name},
                    limit="1",
                ),
            )
            query_run_count = json.loads(resp)[0]["history.query_run_count"]

            formatted_results.append(
                {
                    "Connection": connection.name,
                    "Status": "OK" if not errors else "\n".join(errors),
                    "Query Count": query_run_count,
                }
            )
        self._tabularize_and_print(formatted_results)

    @spinner.Spinner()
    def check_dashboard_performance(self):
        """Prints a list of dashboards with slow running queries in the past
        7 days"""
        print(
            "\bTest 2/6: Checking for dashboards with queries slower than "
            "30 seconds in the last 7 days"
        )
        request = models.WriteQuery(
            model="i__looker",
            view="history",
            fields=["dashboard.title, query.count"],
            filters={
                "history.created_date": "7 days",
                "history.real_dash_id": "-NULL",
                "history.runtime": ">30",
                "history.status": "complete",
            },
            sorts=["query.count desc"],
            limit=20,
        )
        resp = self.sdk.run_inline_query("json", request)
        slowest_dashboards = json.loads(resp)
        self._tabularize_and_print(slowest_dashboards)

    @spinner.Spinner()
    def check_dashboard_errors(self):
        """Prints a list of erroring dashboard queries."""
        print(
            "\bTest 3/6: Checking for dashboards with erroring queries in the last 7 days"  # noqa: B950
        )
        request = models.WriteQuery(
            model="i__looker",
            view="history",
            fields=["dashboard.title", "history.query_run_count"],
            filters={
                "dashboard.title": "-NULL",
                "history.created_date": "7 days",
                "history.dashboard_session": "-NULL",
                "history.status": "error",
            },
            sorts=["history.query_run_ount desc"],
            limit=20,
        )
        resp = self.sdk.run_inline_query("json", request)
        erroring_dashboards = json.loads(resp)
        self._tabularize_and_print(erroring_dashboards)

    @spinner.Spinner()
    def check_explore_performance(self):
        """Prints a list of the slowest running explores."""
        print("\bTest 4/6: Checking for the slowest explores in the past 7 days")
        request = models.WriteQuery(
            model="i__looker",
            view="history",
            fields=["query.model", "query.view", "history.average_runtime"],
            filters={
                "history.created_date": "7 days",
                "query.model": "-NULL, -system^_^_activity",
            },
            sorts=["history.average_runtime desc"],
            limit=20,
        )
        resp = self.sdk.run_inline_query("json", request)
        slowest_explores = json.loads(resp)

        request.fields = ["history.average_runtime"]
        resp = json.loads(self.sdk.run_inline_query("json", request))
        avg_query_runtime = resp[0]["history.average_runtime"]
        if avg_query_runtime:
            print(
                f"\bFor context, the average query runtime is {avg_query_runtime:.4f}s"
            )

        self._tabularize_and_print(slowest_explores)

    @spinner.Spinner()
    def check_schedule_failures(self):
        """Prints a list of schedules that have failed in the past 7 days."""
        print("\bTest 5/6: Checking for failing schedules")
        request = models.WriteQuery(
            model="i__looker",
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
        self._tabularize_and_print(failed_schedules)

    @spinner.Spinner()
    def check_legacy_features(self):
        """Prints a list of enabled legacy features."""
        print("\bTest 6/6: Checking for enabled legacy features")
        lf = list(filter(lambda f: f.enabled, self.sdk.all_legacy_features()))
        legacy_features = [{"Feature": cast(str, f.name)} for f in lf]
        self._tabularize_and_print(legacy_features)
