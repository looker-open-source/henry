import argparse

import pytest  # type: ignore

from henry import cli


@pytest.fixture(name="parser")
def initialize_parser() -> argparse.ArgumentParser:
    return cli.setup_cli()


def test_parse_input_with_pulse(parser: argparse.ArgumentParser):
    """parse_input should assign the right defaults."""
    ip = parser.parse_args(["pulse"])
    assert ip.command == "pulse"
    assert ip.config_file == "looker.ini"
    assert ip.section == "Looker"
    assert ip.timeout == 120

    ip = parser.parse_args(
        ["pulse", "--config-file=some_file.ini", "--section=some_section"]
    )
    assert ip.command == "pulse"
    assert ip.config_file == "some_file.ini"
    assert ip.section == "some_section"
    assert ip.timeout == 120
