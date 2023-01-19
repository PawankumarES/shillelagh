"""
An adapter to EverstageAPI (https://www.weatherapi.com/).
"""
import logging
import urllib.parse
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union, cast
from shillelagh.adapters.registry import registry

import dateutil.tz
import requests_cache

from shillelagh.adapters.base import Adapter
from shillelagh.exceptions import ImpossibleFilterError
from shillelagh.fields import DateTime, Float, IntBoolean, Integer, Order, String
from shillelagh.filters import Filter, Impossible, Operator, Range
from shillelagh.typing import RequestedOrder, Row

_logger = logging.getLogger(__name__)


class EverstageAPI(Adapter):
    """
    An adapter for EverstageAPI
    """

    safe = True

    @staticmethod
    def supports(uri: str, fast: bool = True, **kwargs: Any):
        return True

    @staticmethod
    def parse_uri(uri:str):
        return uri
    
    def __init__(self, api_key:str = None):
        super().__init__()
        self.api_key = api_key
        self._session = requests_cache.CachedSession(
            cache_name="everstageapi_cache",
            backend="sqlite",
            expire_after=180,
        )
    
    def get_data(
        self,
        bounds: Dict[str, Filter],
        order: List[Tuple[str, RequestedOrder]],
        **kwargs: Any,
    ) -> Iterator[Row]:
        url = "https://localhost:8000"
        params = {"key": self.api_key}

        query_string = urllib.parse.urlencode(params)
        _logger.info("GET %s?%s", url, query_string)

        response = self._session.get(url, params=params)
        if response.ok:
            res = response.json()
            return res['data']
        else:
            _logger.info(f'Error from API')
    
    
    def _set_columns(self) -> None:
        rows = list(self.get_data())
        column_names = rows.columns # rows will be a pandas dataframe
        self.columns = column_names
        # self.columns = {
        #     column_name: str
        #     for column_name in column_names
        # }

    def get_columns(self):
        return self.columns


registry.add('everstageapi', EverstageAPI)