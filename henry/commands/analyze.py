from typing import cast, Dict, MutableSequence, Optional, Union, Sequence

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
        analyze.output(data=cast(TResult, result))

    @spinner.Spinner()
    def projects(self, *, id: Optional[str] = None) -> TResult:
        """Analyzes all projects or a specific project."""
        projects: Sequence[models.Project]
        if id:
            projects = [self.sdk.project(id)]
        else:
            projects = self.sdk.all_projects()

        if not projects:
            raise exceptions.NotFoundError("No projects found.")

        result = []
        for p in projects:
            p_files = self.sdk.all_project_files(cast(str, p.id))
            result.append(
                {
                    "Project Name": cast(str, p.id),
                    "# Models": sum(map(lambda p: p.type == "model", p_files)),
                    "# Views": sum(map(lambda p: p.type == "view", p_files)),
                    "Git Connection Status": self.run_git_connection_tests(
                        cast(str, p.id)
                    ),
                    "PR Mode": cast(str, p.pull_request_mode),
                    "Is Validation Required": cast(bool, p.validation_required),
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
            result.append(
                {
                    "Project": cast(str, m.project_name),
                    "Model": m.name,
                    "# Explores": len(cast(Sequence, m.explores)),
                    "# Unused Explores": len(self.get_unused_explores(model=m.name)),
                    "Query Run Count": self.get_used_models().get(m.name) or 0,
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
            field_stats = self.get_explore_field_stats(e)
            join_stats = self.get_explore_join_stats(explore=e, field_stats=field_stats)
            result.append(
                {
                    "Model": cast(str, e.model_name),
                    "Explore": cast(str, e.name),
                    "Is Hidden": cast(bool, e.hidden),
                    "Has Description": "Yes" if e.description else "No",
                    "# Joins": len(join_stats),
                    "# Unused Joins": len(self._filter(join_stats)),
                    "# Fields": len(field_stats),
                    "# Unused Fields": len(self._filter(field_stats)),
                    "Query Count": self.get_used_explores(
                        model=cast(str, e.model_name), explore=cast(str, e.name)
                    ).get(cast(str, e.name), 0),
                }
            )
        return result
