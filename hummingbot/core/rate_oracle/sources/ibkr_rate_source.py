from decimal import Decimal
from typing import TYPE_CHECKING, Dict, Optional
import asyncio

from hummingbot.core.rate_oracle.sources.rate_source_base import RateSourceBase
from hummingbot.core.utils import async_ttl_cache
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.connector.exchange.ibkr import ibkr_constants as CONSTANTS

if TYPE_CHECKING:
    from hummingbot.connector.exchange.ibkr.ibkr_exchange import IbkrExchange


class IbkrRateSource(RateSourceBase):
    def __init__(self):
        super().__init__()
        self._ibkr_exchange: Optional[IbkrExchange] = None  # delayed because of circular reference

    @property
    def name(self) -> str:
        return "ibkr"

    @async_ttl_cache(ttl=30, maxsize=1)
    async def get_prices(self, quote_token: Optional[str] = None) -> Dict[str, Decimal]:
        self._ensure_exchanges()
        results = {}
        tasks = [
            self._get_ibkr_prices(exchange=self._ibkr_exchange),
        ]
        task_results = await safe_gather(*tasks, return_exceptions=True)
        for task_result in task_results:
            if isinstance(task_result, Exception):
                self.logger().error(
                    msg="Unexpected error while retrieving rates from ibkr. Check the log file for more info.",
                    exc_info=task_result,
                )
                break
            else:
                results.update(task_result)
        return results

    def _ensure_exchanges(self):
        if self._ibkr_exchange is None:
            self._ibkr_exchange = self._build_ibkr_connector_without_auth(CONSTANTS.DEFAULT_DOMAIN)

    @staticmethod
    async def _get_ibkr_prices(exchange: 'IbkrExchange') -> Dict[str, Decimal]:
        """
        Fetches ibkr prices

        :param exchange: The exchange instance from which to query prices.
        :return: A dictionary of trading pairs and prices
        """
        try:
            pairs_prices = await exchange.get_all_pairs_prices()
            results = {}
            for pair_price in pairs_prices:
                bid_price = pair_price.get("bidPrice")
                ask_price = pair_price.get("askPrice")
                if bid_price and ask_price and 0 < Decimal(bid_price) <= Decimal(ask_price):
                    results[pair_price['symbol']] = (Decimal(bid_price) + Decimal(ask_price)) / Decimal("2")

            return results
        except Exception as e:
            raise ValueError(e)

    @staticmethod
    def _build_ibkr_connector_without_auth(domain: str) -> 'IbkrExchange':
        from hummingbot.client.hummingbot_application import HummingbotApplication
        from hummingbot.connector.exchange.ibkr.ibkr_exchange import IbkrExchange

        app = HummingbotApplication.main_application()
        client_config_map = app.client_config_map

        return IbkrExchange(
            client_config_map=client_config_map,
            ibkr_api_key="Yes",
            ibkr_secret_key=CONSTANTS.DEFAULT_EXCHANGE,
            trading_pairs=[],
            trading_required=True,
            domain=domain,
        )
