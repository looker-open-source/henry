import yaml
import pytest  # type: ignore

from looker_sdk.sdk.api40 import methods, models
import looker_sdk as client


@pytest.fixture(scope="session")
def test_project_name(test_data):
    return test_data["project"]


@pytest.fixture(scope="session")
def test_model(test_data):
    return test_data["model"]


@pytest.fixture(scope="session")
def test_used_explores(test_model):
    explores = []
    for e in test_model["explores"]:
        if not e.get("unused", False):
            explores.append(e)
    return explores


@pytest.fixture(scope="session")
def test_used_explore_names(test_used_explores):
    explores = []
    for e in test_used_explores:
        explores.append(e["name"])
    return explores


@pytest.fixture(scope="session")
def test_unused_explores(test_model):
    explores = []
    for e in test_model["explores"]:
        if e.get("unused", False):
            explores.append(e["name"])
    return sorted(explores)


@pytest.fixture(scope="session")
def _dimensions_or_measures_only_explores(test_model):
    explores = []
    for e in test_model["explores"]:
        if e.get("dimensions_only", False) or e.get("measures_only", False):
            explores.append(e)
    return explores


@pytest.fixture(params=["dimensions_only", "measures_only"])
def test_dimensions_or_measures_only_explores(
    request, _dimensions_or_measures_only_explores
):
    return list(
        filter(
            lambda e: e.get(request.param, False),
            _dimensions_or_measures_only_explores,
        )
    )


def used_fields(explore):
    if explore.get("unused", False):
        used_fields = ""
    else:
        used_fields = []
        for f in explore["fields"]:
            if not f.get("unused", False):
                used_fields.append(f["name"])
    used_fields = sorted(used_fields) if len(used_fields) > 0 else ""
    return used_fields


def unused_fields(explore):
    unused_fields = []
    if explore.get("unused", False):
        unused_fields = all_fields(explore)
    else:
        unused_fields = []
        for f in explore["fields"]:
            if f.get("unused", False):
                unused_fields.append(f["name"])
    unused_fields = sorted(unused_fields) if len(unused_fields) > 0 else ""
    return unused_fields


def all_fields(explore):
    all_fields = []
    for f in explore["fields"]:
        all_fields.append(f["name"])
    all_fields = sorted(all_fields) if len(all_fields) > 0 else ""
    return all_fields


def unused_joins(explore):
    unused_joins = []
    if explore.get("unused"):
        unused_joins = all_joins(explore)
    elif explore.get("joins"):
        unused_joins = []
        for j in explore["joins"]:
            if j.get("unused", False):
                unused_joins.append(j["name"])
    unused_joins = sorted(unused_joins) if len(unused_joins) > 0 else ""
    return unused_joins


def all_joins(explore):
    all_joins = []
    if explore.get("joins", False):
        for j in explore["joins"]:
            all_joins.append(j["name"])
    all_joins = sorted(all_joins) if len(all_joins) > 0 else ""
    return all_joins


@pytest.fixture(scope="session")
def test_explores_stats(test_model):
    explores = []
    for e in test_model["explores"]:
        explores.append(
            {
                "name": e["name"],
                "all_joins": all_joins(e),
                "unused_joins": unused_joins(e),
                "all_fields": all_fields(e),
                "used_fields": used_fields(e),
                "unused_fields": unused_fields(e),
            }
        )
    return explores


@pytest.fixture(scope="session")
def test_unused_model(test_data):
    return test_data["unused_model"]


@pytest.fixture(scope="session")
def test_unused_model_explores(test_unused_model):
    unused_explores = [
        e for e in test_unused_model["explores"] if e.get("unused", False)
    ]
    return unused_explores


@pytest.fixture(scope="session")
def test_unused_model_explore_names(test_unused_model_explores):
    return [e["name"] for e in test_unused_model_explores]


@pytest.fixture(scope="session", autouse=True)
def run_queries(looker_sdk: methods.Looker40SDK, test_model):
    test_model_name = test_model["name"]
    for e in test_model["explores"]:
        if e.get("unused", False):
            continue
        fields_to_query = [
            f["name"]
            for f in e["fields"]
            if not (f.get("unused", False) or f.get("filter_only", False))
        ]
        filters = {
            f["name"]: f["value"] for f in e["fields"] if f.get("filter_only", False)
        }
        looker_sdk.run_inline_query(
            result_format="json",
            body=models.WriteQuery(
                model=test_model_name,
                view=e["name"],
                fields=fields_to_query,
                filters=filters,
            ),
        )


@pytest.fixture(scope="session", name="test_data")
def _get_test_data():
    with open("tests/data/data.yml") as f:
        test_data = yaml.safe_load(f)
    return test_data


@pytest.fixture(scope="session")
def looker_sdk() -> methods.Looker40SDK:
    looker_sdk = client.init40("looker.ini")
    return looker_sdk
