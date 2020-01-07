# data_controller.py
from typing import Optional, NamedTuple, Sequence, Tuple


class Input(NamedTuple):
    command: str
    subcommand: Optional[str] = None
    project: Optional[str] = None
    model: Optional[str] = None
    explore: Optional[str] = None
    timeframe: Optional[int] = 90
    min_queries: Optional[int] = 0
    sortkey: Optional[Tuple[str, str]] = None
    limit: Optional[Sequence[int]] = None
    config_file: str = "looker.ini"
    section: str = "looker"
    quiet: bool = False
    save: Optional[bool] = False
    timeout: Optional[int] = 120
