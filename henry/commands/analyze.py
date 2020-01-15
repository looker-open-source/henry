from typing import Dict, MutableSequence, Optional, Union, Sequence

from looker_sdk import models

from henry.modules import spinner
from henry.modules import fetcher
from henry.modules import exceptions


TResult = fetcher.TResult


class Analyze(fetcher.Fetcher):
    @classmethod
    def run(cls, user_input: fetcher.Input):
        analyze = cls(user_input)
        if user_input.subcommand == "projects":
            result = analyze.projects(id=user_input.project)
        elif user_input.subcommand == "models":
            result = analyze.models(project=user_input.project, model=user_input.model)
        elif user_input.subcommand == "explores":
            result = analyze.explores(
                model=user_input.model, explore=user_input.explore
            )
        analyze.output(data=result)

    @spinner.Spinner()
    def projects(self, *, id: Optional[str] = None) -> TResult:
        """Analyzes all projects or a specific project."""
        if id:
            projects = [self.sdk.project(id)]
        else:
            projects = self.sdk.all_projects()

        if not projects:
            raise exceptions.NotFoundError("No projects found.")

        result = []
        for p in projects:
            assert isinstance(p.id, str)
            assert isinstance(p.pull_request_mode, str)
            assert isinstance(p.validation_required, bool)
            p_files = self.sdk.all_project_files(p.id)
            result.append(
                {
                    "Project Name": p.id,
                    "# Models": sum(map(lambda p: p.type == "model", p_files)),
                    "# Views": sum(map(lambda p: p.type == "view", p_files)),
                    "Git Connection Status": self.run_git_connection_tests(p.id),
                    "PR Mode": p.pull_request_mode,
                    "Is Validation Required": p.validation_required,
                }
            )
        return result

    @spinner.Spinner()
    def models(
        self, *, project: Optional[str] = None, model: Optional[str] = None
    ) -> TResult:
        """Analyze models, can optionally filter by project or model."""
        all_models = self.get_models(project=project, model=model)
        result: MutableSequence[Dict[str, Union[str, int]]] = []
        for m in all_models:
            assert isinstance(m.name, str)
            assert isinstance(m.project_name, str)
            assert isinstance(m.explores, list)
            result.append(
                {
                    "Project": m.project_name,
                    "Model": m.name,
                    "# Explores": len(m.explores),
                    "# Unused Explores": len(self.get_unused_explores(model=m.name)),
                    "Query Count": self.get_used_models().get(m.name) or 0,
                }
            )
        return result

    @spinner.Spinner()
    def explores(
        self, *, model: Optional[str] = None, explore: Optional[str] = None
    ) -> TResult:
        """Analyze explores."""
        all_explores = self.get_explores(model=model, explore=explore)
        result: fetcher.TResult = []
        for e in all_explores:
            assert isinstance(e.name, str)
            assert isinstance(e.model_name, str)
            assert isinstance(e.hidden, bool)
            field_stats = self.get_explore_field_stats(e)
            join_stats = self.get_explore_join_stats(explore=e, field_stats=field_stats)
            result.append(
                {
                    "Model": e.model_name,
                    "Explore": e.name,
                    "Is Hidden": e.hidden,
                    "Has Description": "Yes" if e.description else "No",
                    "# Joins": len(join_stats),
                    "# Unused Joins": len(self._filter(join_stats)),
                    "# Fields": len(field_stats),
                    "# Unused Fields": len(self._filter(field_stats)),
                    "Query Count": self.get_used_explores(
                        model=e.model_name, explore=e.name
                    ).get(e.name, 0),
                }
            )
        return result
