from __future__ import annotations

import uuid
from henry.modules.lookerapi import LookerApi
from typing import List

_DEFAULTS = {'plain': None,
             'model': None,
             'explore': None,
             'min_queries': 0,
             'timeframe': 90,
             'limit': None,
             'sortkey': None
             }


class Looker:
    def api(**kwargs) -> LookerApi:
        kwargs['id'] = kwargs.pop('client_id')
        kwargs['secret'] = kwargs.pop('client_secret')
        return LookerApi(**kwargs)

    def sdk(**kwargs):
        """TODO connect to looker_sdk and/or interface"""
        pass


class Henry:

    def __init__(self, **creds):
        self.args = _DEFAULTS
        self.args.update(**creds)

        self.sdk = Looker.sdk(**creds)  # looker_sdk interface
        session_info: str = f'Henry v0.1.3: sid=#{uuid.uuid1()}'
        creds.update(session_info=session_info)
        self.looker = Looker.api(**creds)  # Henry interface

    def __call__(self):
        return self.looker

    def _copy_args(self, **kwargs) -> dict:
        _args = self.args.copy()
        _args.update(**kwargs)
        return _args

    @property
    def pulse(self):
        from henry.commands.pulse import Pulse

        return Pulse(self.looker)

    @property
    def analyzer(self):
        from henry.commands.analyze import Analyze

        return Analyze(self.looker)

    def analyze(self, **kwargs):
        kwargs.update(command='analyze')
        _args = self._copy_args(**kwargs)

        return self.analyzer.analyze(**_args)

    @property
    def scanner(self):
        from henry.commands.vacuum import Vacuum

        return Vacuum(self.looker)

    def vacuum(self, **kwargs) -> List[dict]:
        kwargs.update(command='vacuum')
        _args = self._copy_args(**kwargs)

        return self.scanner.vacuum(**_args)

    def to_df(self, data, **kwargs):
        import pandas as pd

        return pd.DataFrame(data, **kwargs)
