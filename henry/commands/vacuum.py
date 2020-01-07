from typing import cast, Optional

from henry.modules import fetcher
from henry.modules import spinner


class Vacuum(fetcher.Fetcher):
    @classmethod
    def run(cls, user_input: fetcher.Input):
        vacuum = cls(user_input)
        if user_input.subcommand == "models":
            result = vacuum.models(project=user_input.project, model=user_input.model)
        elif user_input.subcommand == "explores":
            result = vacuum.explores(model=user_input.model, explore=user_input.explore)
        vacuum.output(data=cast(fetcher.TResult, result))

    @spinner.Spinner()
    def models(self, *, project, model) -> fetcher.TResult:
        """Analyze models."""
        all_models = self.get_models(project=project, model=model)
        used_models = self.get_used_models()
        result: fetcher.TResult = []
        for m in all_models:
            assert isinstance(m.name, str)
            result.append(
                {
                    "Model Name": m.name,
                    "Unused Explores": "\n".join(
                        sorted(self.get_unused_explores(m.name))
                    ),
                    "Query Count": used_models.get(m.name, 0),
                }
            )
        return result

    @spinner.Spinner()
    def explores(
        self, *, model: Optional[str] = None, explore: Optional[str] = None
    ) -> fetcher.TResult:
        """Analyze explores"""
        explores = self.get_explores(model=model, explore=explore)

        result: fetcher.TResult = []
        for e in explores:
            field_stats = self.get_explore_field_stats(e)
            join_stats = self.get_explore_join_stats(explore=e, field_stats=field_stats)
            result.append(
                {
                    "Model": cast(str, e.model_name),
                    "Explore": cast(str, e.name),
                    "Unused Joins": "\n".join(sorted(self._filter(join_stats).keys())),
                    "Unused Fields": "\n".join(sorted(self._filter(field_stats))),
                }
            )
        return result
