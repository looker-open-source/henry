from typing import Callable, Dict, Optional, Sequence, Tuple, Union

import pytest  # type: ignore
from looker_sdk.sdk.api40 import methods, models

from henry.modules import exceptions, fetcher


@pytest.fixture(name="fc")
def initialize() -> fetcher.Fetcher:
    """Returns an instance of fetcher"""
    options = fetcher.Input(
        command="some_cmd", config_file="looker.ini", section="Looker"
    )
    return fetcher.Fetcher(options)


def test_get_projects_returns_projects(fc: fetcher.Fetcher):
    """fetcher.get_projects() should return a list of projects."""
    projects = fc.get_projects()
    assert isinstance(projects, list)
    assert isinstance(projects[0], models.Project)


def test_get_projects_filters(fc: fetcher.Fetcher, test_project_name):
    """fetchet.get_projects() should be able to filter on project."""
    projects = fc.get_projects(test_project_name)
    assert isinstance(projects, list)
    assert len(projects) == 1
    assert projects[0].name == test_project_name


def test_get_projects_throws_if_project_does_not_exist(fc: fetcher.Fetcher):
    """fetchet.get_projects() should error if filter is invalid"""
    with pytest.raises(exceptions.NotFoundError) as exc:
        fc.get_projects("BadProject")
    assert "An error occured while getting projects." in str(exc.value)


def test_get_models_returns_models(fc: fetcher.Fetcher):
    """fetcher.get_models() should return a list of models."""
    ml = fc.get_models()
    assert isinstance(ml, list)
    assert isinstance(ml[0], models.LookmlModel)


def test_get_models_filters(fc: fetcher.Fetcher, test_project_name, test_model):
    """fetcher.get_models() should be able to filter on project or model."""
    ml = fc.get_models(project=test_project_name)
    assert all(m.project_name == test_project_name for m in ml)

    ml = fc.get_models(model=test_model["name"])
    assert all(m.name == test_model["name"] for m in ml)

    ml = fc.get_models(project=test_project_name, model=test_model["name"])
    assert all(
        m.project_name == test_project_name and m.name == test_model["name"] for m in ml
    )


@pytest.mark.parametrize(
    "project, model", [(None, "BadModel")],
)
def test_get_models_throws_if_model_does_not_exist(fc: fetcher.Fetcher, project, model):
    """fetcher.get_models() should throw if a model is not found."""
    with pytest.raises(exceptions.NotFoundError) as exc:
        fc.get_models(project=project, model=model)
    assert "An error occured while getting models." in str(exc.value)


@pytest.mark.parametrize(
    "project, model", [("BadProject", None), ("BadProject", "BadModel")],
)
def test_get_models_throws_if_project_does_not_exist(
    fc: fetcher.Fetcher, project, model
):
    """fetcher.get_models() should throw if a model is not found."""
    with pytest.raises(exceptions.NotFoundError) as exc:
        fc.get_models(project=project, model=model)
    assert "An error occured while getting projects." in str(exc.value)


def test_get_used_models(fc: fetcher.Fetcher, test_model):
    """fetcher.get_used_models() should return models that have queries against them."""
    used_models = fc.get_used_models()
    assert isinstance(used_models, dict)
    assert len(used_models) > 0
    assert all(type(model_name) == str for model_name in used_models.keys())
    assert all(type(query_count) == int for query_count in used_models.values())
    assert test_model["name"] in used_models.keys()


def test_get_explores(fc: fetcher.Fetcher):
    """fetcher.get_explores() should return a list of explores."""
    explores = fc.get_explores()
    assert isinstance(explores, list)
    assert len(explores) > 0
    assert isinstance(explores[0], models.LookmlModelExplore)


def test_get_explores_filters(fc: fetcher.Fetcher):
    """fetcher.get_explores() should be able to filter on model and/or explore."""
    explores = fc.get_explores(model="henry_dusty")
    assert all(e.model_name == "henry_dusty" for e in explores)

    explores = fc.get_explores(model="henry_qa", explore="explore_2_joins_all_used")
    assert all(
        e.model_name == "henry_qa" and e.name == "explore_2_joins_all_used"
        for e in explores
    )


@pytest.mark.parametrize(
    "model, explore, msg",
    [
        ("non_existing_model", None, "An error occured while getting models."),
        (
            "non_existing_model",
            "non_existing_explore",
            "An error occured while getting models/explores.",
        ),
    ],
)
def test_get_explores_throws_if_model_or_explore_does_not_exist(
    fc: fetcher.Fetcher, model: Optional[str], explore: Optional[str], msg: str
):
    """fetcher.get_explores() should throw if an explore/model is not found."""
    with pytest.raises(exceptions.NotFoundError) as exc:
        fc.get_explores(model=model, explore=explore)
    assert msg in str(exc.value)


def test_get_used_explores(fc: fetcher.Fetcher, test_model, test_used_explore_names):
    """fetcher.get_used_explores() should return all used explores."""
    used_explores = fc.get_used_explores(model=test_model["name"])
    assert isinstance(used_explores, dict)
    assert all(e in test_used_explore_names for e in used_explores)


def test_get_unused_explores(fc: fetcher.Fetcher, test_model, test_unused_explores):
    """fetcher.get_unused_explores() should return all unused explores."""
    unused_explores = fc.get_unused_explores(model=test_model["name"])
    assert all(e in test_unused_explores for e in unused_explores)


def test_get_explore_fields_gets_fields(
    fc: fetcher.Fetcher, test_model, test_explores_stats
):
    """fetcher.get_explore_fields() should return an explores fields."""
    test_explore = test_explores_stats[0]
    explore = fc.get_explores(model=test_model["name"], explore=test_explore["name"])
    assert isinstance(explore, list)
    explore = explore[0]
    assert isinstance(explore, models.LookmlModelExplore)
    assert explore.model_name == test_model["name"]
    assert explore.name == test_explore["name"]
    fields = fc.get_explore_fields(explore)
    assert isinstance(fields, list)
    assert fields == test_explore["all_fields"]


def test_get_explore_fields_gets_fields_for_dimension_or_measure_only_explores(
    fc: fetcher.Fetcher, test_model, test_dimensions_or_measures_only_explores
):
    """fetcher.get_explore_fields() should return when an explore has only dimensions
    or only measures.
    """
    expected = test_dimensions_or_measures_only_explores[0]
    explore = fc.get_explores(model=test_model["name"], explore=expected["name"])
    assert isinstance(explore, list)
    actual = explore[0]
    assert actual.name == expected["name"]
    assert not (actual.fields.dimensions and actual.fields.measures)
    expected_fields = [f["name"] for f in expected["fields"]]
    actual_fields = fc.get_explore_fields(actual)
    assert actual_fields == expected_fields


def test_get_explore_field_stats(
    fc: fetcher.Fetcher,
    looker_sdk: methods.Looker40SDK,
    test_model,
    test_used_explore_names,
    test_explores_stats,
):
    """fetcher.get_explore_field_stats() should get the stats of all fields in
    an explore.
    """
    explore = fc.get_explores(
        model=test_model["name"], explore=test_used_explore_names[0]
    )[0]
    actual_stats = fc.get_explore_field_stats(explore)
    assert isinstance(actual_stats, dict)

    for e in test_explores_stats:
        if e["name"] == test_used_explore_names[0]:
            expected_stats = e

    assert all(actual_stats[k] == 0 for k in expected_stats["unused_fields"])
    assert all(actual_stats[k] > 0 for k in expected_stats["used_fields"])


def test_get_explore_join_stats(fc: fetcher.Fetcher, test_model):
    """fetcher.get_explore_join_stats() should return the stats of all joins in
    an explore.
    """
    explore = fc.get_explores(
        model=test_model["name"], explore="explore_2_joins_1_used"
    )[0]
    field_stats = {
        "explore_2_joins_1_used.d1": 10,
        "explore_2_joins_1_used.d2": 5,
        "explore_2_joins_1_used.d3": 0,
        "explore_2_joins_1_used.m1": 0,
        "join1.d1": 10,
        "join1.d2": 10,
        "join1.d3": 10,
        "join1.m1": 0,
        "join2.d1": 0,
        "join2.d2": 0,
        "join2.d3": 0,
        "join2.m1": 0,
    }
    join_stats = fc.get_explore_join_stats(explore=explore, field_stats=field_stats)
    assert isinstance(join_stats, dict)
    assert len(join_stats) == 2
    assert join_stats == {"join1": 30, "join2": 0}


@pytest.mark.parametrize(
    "limit, input_data, expected_result",
    [
        (
            None,
            [{"a": 1}, {"b": 2}, {"c": 3}, {"d": 4}, {"e": 5}],
            [{"a": 1}, {"b": 2}, {"c": 3}, {"d": 4}, {"e": 5}],
        ),
        (2, [{"a": 1}, {"b": 2}, {"c": 3}, {"d": 4}, {"e": 5}], [{"a": 1}, {"b": 2}]),
    ],
)
def test_limit(
    fc: fetcher.Fetcher,
    limit: Optional[int],
    input_data: Sequence[Dict[str, Union[bool, int, str]]],
    expected_result: Sequence[int],
):
    fc.limit = limit
    result = fc._limit(input_data)
    assert result == expected_result


@pytest.mark.parametrize(
    "data, condition, expected_output",
    [
        ({"e1": 0, "e2": 0, "e3": 5, "e4": 10, "e5": 15}, None, {"e1": 0, "e2": 0},),
        (
            {"e1": 0, "e2": 0, "e3": 5, "e4": 10, "e5": 15},
            lambda x: x[1] >= 10,
            {"e4": 10, "e5": 15},
        ),
        ({"e1": 0, "e2": 0, "e3": 5, "e4": 10, "e5": 15}, lambda x: x[1] >= 100, {},),
        (None, lambda x: x[1] > 0, {}),
        (None, None, {}),
    ],
)
def test_filter(
    fc: fetcher.Fetcher,
    data: Optional[Dict[str, int]],
    condition: Callable,
    expected_output: Dict[str, int],
):
    result = fc._filter(data, condition)
    assert result == expected_output


DATA: Sequence[Dict[str, Union[str, int]]] = [
    {"explore": "a", "join count": 1},
    {"explore": "b", "join count": 0},
    {"explore": "c", "join count": 2},
    {"explore": "d", "join count": 3},
]


@pytest.mark.parametrize(
    "sortkey, expected_output",
    [
        (("explore", "asc"), DATA),
        (
            ("join count", "desc"),
            [
                {"explore": "d", "join count": 3},
                {"explore": "c", "join count": 2},
                {"explore": "a", "join count": 1},
                {"explore": "b", "join count": 0},
            ],
        ),
    ],
)
def test_sort(
    fc: fetcher.Fetcher,
    sortkey: Tuple[str, str],
    expected_output: Sequence[Dict[str, Union[int, str, bool]]],
):
    fc.sortkey = sortkey
    result = fc._sort(DATA)
    assert result == expected_output


@pytest.mark.parametrize(
    "sortkey", [(("explore", "invalid")), (("invalid field", "asc"))]
)
def test_sort_throws_for_invalid_sort_keys(
    fc: fetcher.Fetcher, sortkey: Tuple[str, str]
):
    with pytest.raises(KeyError):
        fc.sortkey = sortkey
        fc._sort(DATA)
