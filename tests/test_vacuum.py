import pytest  # type: ignore

from henry.commands import vacuum
from henry.modules import exceptions, fetcher


@pytest.fixture(name="vacuum")
def initialize() -> vacuum.Vacuum:
    options = fetcher.Input(
        command="some_cmd", config_file="looker.ini", section="Looker"
    )
    return vacuum.Vacuum(options)


def test_vacuum_model_vacuums_test_model(
    vacuum: vacuum.Vacuum, test_project_name, test_model, test_unused_explores,
):
    """vacuum.models() should return unused explores in a used model."""
    result1 = vacuum.models(project=test_project_name, model=test_model["name"])
    result2 = vacuum.models(model=test_model["name"])
    assert result1 == result2
    assert isinstance(result1, list)
    assert len(result1) == 1
    result = result1[0]
    assert result["Model"] == test_model["name"]
    assert result["Unused Explores"] == "\n".join(test_unused_explores)
    assert result["Model Query Count"] > 0


def test_vacuum_models_vacuums_unused_test_model(
    vacuum: vacuum.Vacuum,
    test_project_name,
    test_unused_model,
    test_unused_model_explore_names,
):
    """vacuum.models() should return all explores in unused models."""
    result = vacuum.models(model=test_unused_model["name"])
    assert isinstance(result, list)
    assert len(result) == 1
    result = result[0]
    assert result["Model"] == test_unused_model["name"]
    assert result["Unused Explores"] == "\n".join(test_unused_model_explore_names)
    assert result["Model Query Count"] == 0


@pytest.mark.parametrize(
    "project, model, msg",
    [
        ("BadProject", "henry_qa", "error occured while getting projects."),
        ("henry", "BadModel", "error occured while getting models."),
        ("BadProject", "BadModel", "error occured while getting projects."),
    ],
)
def test_vacuum_models_throws_for_bad_filters(
    vacuum: vacuum.Vacuum, project, model, msg
):
    """vacuum.models() should error for bad project/model filter values."""
    with pytest.raises(exceptions.NotFoundError) as exc:
        vacuum.models(project=project, model=model)
    assert msg in str(exc.value)


def test_vacuum_explores_filters(
    vacuum: vacuum.Vacuum, test_project_name, test_model, test_explores_stats,
):
    """vacuum.explores() should be able to filter models and explores."""
    # Filtering on model
    result = vacuum.explores(model=test_model["name"])
    assert isinstance(result, list)
    assert len(result) == len(test_explores_stats)
    assert all(r["Model"] == test_model["name"] for r in result)

    test_explore_names = [e["name"] for e in test_explores_stats]
    assert all(r["Explore"] in test_explore_names for r in result)

    # Filtering on explore and model
    test_explore = test_explores_stats[0]
    result = vacuum.explores(model=test_model["name"], explore=test_explore["name"])
    assert isinstance(result, list)
    assert len(result) == 1
    result = result[0]
    assert result["Model"] == test_model["name"]
    assert result["Explore"] == test_explore["name"]
    assert result["Unused Joins"] == test_explore["unused_joins"]
    assert result["Unused Fields"] == test_explore["unused_fields"]


@pytest.mark.parametrize(
    "test_explore",
    [
        "explore_2_joins_all_used",
        "explore_2_joins_1_used",
        "unused_explore_2_joins",
        "unused_explore_no_joins",
    ],
)
def test_vacuum_explores_vacuums(
    vacuum: vacuum.Vacuum, test_model, test_explore, test_explores_stats,
):
    """vacuum.explores() should return the unused joins and fields for a given
    explore.
    """
    result = vacuum.explores(model=test_model["name"], explore=test_explore)
    assert isinstance(result, list)
    assert len(result) == 1
    result = result[0]

    test_explore_stats = list(
        filter(lambda e: e["name"] == test_explore, test_explores_stats)
    )[0]
    assert result["Model"] == test_model["name"]
    assert result["Explore"] == test_explore
    assert result["Unused Joins"] == "\n".join(test_explore_stats["unused_joins"])
    assert result["Unused Fields"] == "\n".join(test_explore_stats["unused_fields"])


@pytest.mark.parametrize(
    "model, explore, msg",
    [
        ("BadModel", None, "error occured while getting models."),
        (
            "BadModel",
            "explore_2_joins_used",
            "error occured while getting models/explores.",
        ),
        ("BadModel", "BadExplore", "error occured while getting models/explores"),
        ("henry_qa", "BadExplore", "error occured while getting models/explores"),
    ],
)
def test_vacuum_explores_throws_for_bad_filters(
    vacuum: vacuum.Vacuum, model, explore, msg
):
    """vacuum.explores() should error for bad model/explore filter values."""
    with pytest.raises(exceptions.NotFoundError) as exc:
        vacuum.explores(model=model, explore=explore)
    assert msg in str(exc.value)
