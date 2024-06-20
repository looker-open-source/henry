from __future__ import annotations

import uuid
from henry.modules.lookerapi import LookerApi
import looker_sdk
from typing import List

_DEFAULTS = {
    "plain": None,
    "model": None,
    "explore": None,
    "min_queries": 0,
    "timeframe": 90,
    "limit": None,
    "sortkey": None,
}


class Looker:
    def henry_api(**kwargs) -> LookerApi:
        kwargs["id"] = kwargs.pop("client_id")
        kwargs["secret"] = kwargs.pop("client_secret")

        session_info = f"Henry v0.1.3: sid=#{uuid.uuid1()}"
        kwargs.update(session_info=session_info)

        return LookerApi(**kwargs)


class Henry:
    def __init__(self, **creds):
        self.args = _DEFAULTS
        self.args.update(**creds)

        self.api = Looker.henry_api(**creds)  # Henry interface to Looker

    def _update_args(self, **kwargs) -> dict:
        _args = self.args.copy()
        _args.update(**kwargs)
        return _args

    @property
    def pulse(self):
        from henry.commands.pulse import Pulse

        return Pulse(self.api)

    @property
    def analyzer(self):
        from henry.commands.analyze import Analyze

        return Analyze(self.api)

    def analyze(self, **kwargs):
        """analyze explores or models"""
        _args = self._update_args(**kwargs)

        return self.analyzer.analyze(command='analyze', **_args)

    @property
    def scanner(self):
        from henry.commands.vacuum import Vacuum

        return Vacuum(self.api)

    def vacuum(self, **kwargs) -> List[dict]:
        """vacuum explores or models"""
        _args = self._update_args(**kwargs)

        return self.scanner.vacuum(command='vacuum', **_args)

    @classmethod
    def to_df(data, **kwargs):
        import pandas as pd

        return pd.DataFrame(data, **kwargs)
