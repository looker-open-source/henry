import pytest  # type: ignore

from henry.commands import analyze
from henry.modules import exceptions, fetcher


@pytest.fixture(name="analyze")
def initialize() -> analyze.Analyze:
    options = fetcher.Input(
        command="some_cmd", config_file="looker.ini", section="Looker"
    )
    return analyze.Analyze(options)


def test_analyze_projects(analyze: analyze.Analyze, test_project_name: str):
    """analyze.projects() should return information about a project."""
    result = analyze.projects(id=test_project_name)
    assert isinstance(result, list)
    assert len(result) == 1
    result = result[0]
    assert result["Project"] == test_project_name
    assert result["# Models"] == 3
    assert result["# View Files"] == 1
    assert result["PR Mode"] == "off"
    assert result["Is Validation Required"] is True


def test_analyze_projects_throws_for_bad_project_names(analyze: analyze.Analyze):
    """analyze.projects() should throw if a project does not exist."""
    with pytest.raises(exceptions.NotFoundError) as exc:
        analyze.projects(id="BadProjectName")
    assert "An error occured while getting projects." in str(exc.value)


def test_analyze_models(
    analyze: analyze.Analyze,
    test_project_name,
    test_model,
    test_explores_stats,
    test_used_explores,
):
    """analyze.models() should return information about a model."""
    result = analyze.models(model=test_model["name"])
    assert isinstance(result, list)
    assert len(result) == 1
    result = result[0]
    assert result["Project"] == test_project_name
    assert result["Model"] == test_model["name"]
    assert result["# Explores"] == len(test_explores_stats)
    assert result["# Unused Explores"] == len(test_explores_stats) - len(
        test_used_explores
    )
    assert result["Query Count"] > 0


def test_analyze_models_throws_for_bad_project_names(analyze: analyze.Analyze):
    """analyze.models() should throw for projects that do not exist."""
    with pytest.raises(exceptions.NotFoundError) as exc:
        analyze.models(project="BadProjectName")
    assert "An error occured while getting projects." in str(exc.value)


def test_analyze_models_throws_for_bad_model_names(analyze: analyze.Analyze):
    """analyze.models() should throw for models that do not exist."""
    with pytest.raises(exceptions.NotFoundError) as exc:
        analyze.models(model="BadModelName")
    assert "An error occured while getting models." in str(exc.value)


def test_analyze_explores(analyze: analyze.Analyze, test_model, test_used_explores):
    """analyze.explores() should return information about explores."""
    test_used_explore = test_used_explores[1]
    result = analyze.explores(
        model=test_model["name"], explore=test_used_explore["name"]
    )
    assert isinstance(result, list)
    assert len(result) == 1

    result = result[0]
    assert result["Model"] == test_model["name"]
    assert result["Explore"] == test_used_explore["name"]
    assert result["Is Hidden"] == test_used_explore["hidden"]
    assert result["Has Description"] == test_used_explore["description"]
    assert result["# Joins"] == len(test_used_explore.get("joins", []))
    assert result["# Unused Joins"] == len(
        [j for j in test_used_explore.get("joins", []) if j.get("unused", False)]
    )
    assert result["# Fields"] == len(test_used_explore["fields"])
    assert result["# Unused Fields"] == len(
        [f for f in test_used_explore["fields"] if f.get("unused", False)]
    )
    assert result["Query Count"] > 0


def test_analyze_explores_throws_for_bad_model_names(analyze: analyze.Analyze):
    """analyze.explores() should error for wrong model filters."""
    with pytest.raises(exceptions.NotFoundError) as exc:
        analyze.explores(model="BadModelName")
    assert "An error occured while getting models." in str(exc.value)


def test_analyze_explores_throws_for_bad_explore_names(
    analyze: analyze.Analyze, test_model
):
    """analyze.explores() should error for wrong explore filters."""
    with pytest.raises(exceptions.NotFoundError) as exc:
        analyze.explores(model=test_model["name"], explore="BadExploreName")
    assert "An error occured while getting models/explores." in str(exc.value)
