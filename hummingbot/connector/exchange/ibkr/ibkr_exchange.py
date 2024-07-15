import asyncio

import aiohttp
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Callable
from datetime import datetime, timezone, timedelta

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN, TWELVE_HOURS
from hummingbot.connector.exchange.ibkr import (
    ibkr_constants as CONSTANTS,
    ibkr_utils,
    ibkr_web_utils as web_utils,
)
from hummingbot.connector.exchange.ibkr.ibkr_api_order_book_data_source import IbkrAPIOrderBookDataSource
from hummingbot.connector.exchange.ibkr.ibkr_auth import IbkrAuth
from hummingbot.connector.exchange.ibkr.ibkr_api_user_stream_data_source import IbkrAPIUserStreamDataSource
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.connector.utils import TradeFillOrderDetails
from hummingbot.core.event.events import MarketEvent, OrderFilledEvent
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.connector.constants import s_decimal_0

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter


class IbkrExchange(ExchangePyBase):
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 30.0
    TRADING_RULES_INTERVAL = TWELVE_HOURS

    web_utils = web_utils

    def __init__(self,
                 client_config_map: "ClientConfigAdapter",
                 ibkr_api_key: str,
                 ibkr_secret_key: str,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 ):
        self.ibkr_session_auth = ibkr_api_key
        self.ibkr_exchange_name = CONSTANTS.DEFAULT_EXCHANGE
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._domain = domain
        self._account_id = None
        self.trading_pairs_prices = []
        self._orders_list = {}
        self._all_orders = {}
        self._options_data = {}
        self._futures_data = {}
        self._order_place: bool = False
        super().__init__(client_config_map=client_config_map)

    async def _get_account_id(self) -> str:
        """
            Account id is necessary for orders end points
        """
        try:
            if await ibkr_utils.is_authenticated_user(api_get=self._api_get, path_url=CONSTANTS.AUTH_CONFIRM_PATH_URL):
                resp = await self._api_get(path_url=CONSTANTS.ACCOUNT_PATH_URL, limit_id=CONSTANTS.ACCOUNT_PATH_URL)
                return resp['accounts'][0]
            else:
                raise ValueError(CONSTANTS.AUTH_EXCEPTION)
        except Exception as e:
            raise Exception(e)

    async def check_account_id_status(self):
        """
            Checking whether account id is ready or not
        """
        try:
            if self._account_id:
                return self._account_id
            else:
                self._account_id = await self._get_account_id()
                return self._account_id
        except Exception as e:
            raise Exception(e)

    async def check_network_status(self):
        try:
            session_data = await self._make_network_check_request()
            auth = session_data.get("iserver").get("authStatus")
            if auth.get("authenticated") and auth.get("connected"):
                return True
            else:
                return False
        except Exception:
            return True

    async def _make_network_check_request(self):
        return await self._api_get(path_url=self.check_network_request_path, limit_id=self.check_network_request_path)

    async def check_network(self) -> NetworkStatus:
        """
        Checks connectivity with the exchange using the API
        """
        try:
            session_data = await self._make_network_check_request()
            auth = session_data.get("iserver").get("authStatus")
            if auth.get("authenticated") and auth.get("connected"):
                return NetworkStatus.CONNECTED
            else:
                return NetworkStatus.NOT_CONNECTED
        except Exception:
            return NetworkStatus.NOT_CONNECTED

    async def _all_exchange_pairs(self) -> List[str]:
        """
        List of all trading pairs supported by the connector

        :return: List of trading pair symbols in the Hummingbot format
        """
        mapping = await self.trading_pair_symbol_map()
        return list(mapping.keys())

    async def _get_trading_pair_tuples(self):
        trading_pair_tuples = []
        for trading_pair in self._trading_pairs:
            try:
                con_id = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                trading_pair_tuples.append((con_id, trading_pair))
            except Exception as e:
                continue
        return trading_pair_tuples

    @property
    def domain(self):
        return self._domain

    @staticmethod
    def ibkr_order_type(order_type: OrderType) -> str:
        return order_type.name.upper()

    @staticmethod
    def to_hb_order_type(ibkr_type: str) -> OrderType:
        return OrderType[ibkr_type]

    @property
    def client_order_id_max_length(self):
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self):
        return CONSTANTS.HBOT_ORDER_ID_PREFIX
    
    @property
    def name(self) -> str:
        return "ibkr"

    @property
    def authenticator(self):
        return IbkrAuth()

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS
    
    @property
    def trading_rules_request_path(self):
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.GET_ALL_CONRACTS_EXCHANGE
    
    @property
    def exchange_permission_request_path(self):
        return CONSTANTS.MY_TRADES_PATH_URL

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        return False

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return False

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return False

    @staticmethod
    def _match_trade_with_order(trade, order):
        # Convert order and trade times to timestamps
        trade_timestamp = ibkr_utils.convert_str_time_to_timestamp(trade['trade_time'], '%Y%m%d-%H:%M:%S')
        order_timestamp = ibkr_utils.convert_str_time_to_timestamp(order['order_time'], '%y%m%d%H%M%S')

        # Matching criteria
        if (
                trade_timestamp == order_timestamp and
                trade['size'] == float(order['total_size']) and
                trade['price'] == order['average_price'] and
                trade['conid'] == order['conid'] and
                trade['side'] == order['side']
        ):
            return True
        return False
    
    async def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: list[Dict[str, Any]]):
        mapping = bidict()
        for symbol_data in filter(ibkr_utils.is_exchange_information_valid, exchange_info):
            mapping[str(symbol_data["conid"])] = f"{symbol_data['55']}"
            trading_pair_price = dict(
                symbol=symbol_data["55"],
                lastPrice=symbol_data.get("31", "0.0"),
                bidPrice=symbol_data.get("84", "0.0"),
                bidSize=symbol_data.get("88", "0.0"),
                askPrice=symbol_data.get("86", "0.0"),
                askSize=symbol_data.get("85", "0.0")
            )
            self.trading_pairs_prices.append(trading_pair_price)
        self._set_trading_pair_symbol_map(mapping)

    async def _get_historical_market_data(self, con_id: str, period: str, interval: str):
        exchange_info = {}
        params = {"conid": con_id, "exchange": self.ibkr_exchange_name, "period": period,
                  "bar": interval, "outsideRth": "true"}
        results = await self._api_get(path_url=CONSTANTS.MARKET_HISTORY_PATH_URL, params=params,
                                      limit_id=CONSTANTS.MARKET_HISTORY_PATH_URL)
        if results:
            exchange_info[results["symbol"]] = results["data"]
        return exchange_info

    async def get_all_pairs_historical_price(self, period: str, interval: str):
        exchange_historical_price = {}
        try:
            if await ibkr_utils.is_authenticated_user(api_get=self._api_get, path_url=CONSTANTS.AUTH_CONFIRM_PATH_URL):
                con_ids = [await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                           for trading_pair in self._trading_pairs]
                con_ids_chunks = [con_ids[i:i + CONSTANTS.HISTORICAL_CHUNK_SIZE] for i in
                                  range(0, len(con_ids), CONSTANTS.HISTORICAL_CHUNK_SIZE)]
                for chunks in con_ids_chunks:
                    tasks = [self._get_historical_market_data(con_id=con_id, period=period, interval=interval)
                             for con_id in chunks]
                    results = await safe_gather(*tasks)
                    results = {k: v for data in results for k, v in data.items()}
                    exchange_historical_price.update(results)
                return exchange_historical_price
            else:
                self.logger().error(CONSTANTS.AUTH_EXCEPTION)
                return exchange_historical_price
        except Exception as e:
            self.logger().error(e)
        return exchange_historical_price

    async def _get_market_data(self, symbol_chunk) -> List[Dict[str, Any]]:
        exchange_info = []
        params = {
            "conids": ",".join(symbol_chunk),
            "fields": CONSTANTS.MARKET_DATA_FIELDS
        }
        twice_try = 2
        while twice_try > 0:
            exchange_info = await self._api_get(path_url=CONSTANTS.CONTRACT_MARKET_DATA, params=params,
                                                limit_id=CONSTANTS.CONTRACT_MARKET_DATA)
            await self._sleep(3.0)
            twice_try -= 1
        return exchange_info
    
    async def _get_trading_permission(self, exchange_info: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
        exchange_info_list = []
        symbol_pairs = [str(item['conid']) for item in exchange_info]
        chunk_size = 300
        symbol_pairs_chunks = [symbol_pairs[i:i + chunk_size] for i in range(0, len(symbol_pairs), chunk_size)]
        tasks = [self._get_market_data(symbol_chunk=symbol_chunk) for symbol_chunk in symbol_pairs_chunks]
        results = await safe_gather(*tasks)
        for result in results:
            exchange_info_list.extend(result)
        return exchange_info_list
    
    async def _make_trading_pairs_request(self) -> Any:
        exchange_info = {}
        try:
            params = {"exchange": self.ibkr_exchange_name}
            exchange_info = await self._api_get(path_url=self.trading_pairs_request_path, params=params,
                                                limit_id=self.trading_pairs_request_path)
            return exchange_info
        except Exception as e:
            raise Exception(e)
    
    async def _initialize_trading_pair_symbol_map(self):
        try:
            if not self.trading_pair_symbol_map_ready():
                await self.check_account_id_status()
                exchange_info = await self._make_trading_pairs_request()
                if exchange_info:
                    exchange_trading_pairs = await self._get_trading_permission(exchange_info=exchange_info)
                    await self._initialize_trading_pair_symbols_from_exchange_info(exchange_info=exchange_trading_pairs)
                else:
                    raise ValueError(f"No exchange information found for {self.ibkr_exchange_name}")
        except Exception as e:
            raise Exception(e)

    @property
    def check_network_request_path(self):
        return CONSTANTS.PING_PATH_URL

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self) -> List[OrderType]:
        return [OrderType.LIMIT, OrderType.MARKET]

    async def _get_pair_prices(self) -> List[Dict[str, Any]]:
        try:
            if self.trading_pairs_prices:
                return self.trading_pairs_prices
            else:
                await self._initialize_trading_pair_symbol_map()
                return self.trading_pairs_prices
        except Exception as e:
            raise Exception(e)

    async def get_all_pairs_prices(self) -> List[Dict[str, Any]]:
        try:
            if await ibkr_utils.is_authenticated_user(api_get=self._api_get, path_url=CONSTANTS.AUTH_CONFIRM_PATH_URL):
                return await self._get_pair_prices()
        except Exception as e:
            raise Exception(e)

    async def get_trading_pair_price(self, trading_pair: str) -> Dict[str, Any]:
        try:
            if await ibkr_utils.is_authenticated_user(api_get=self._api_get, path_url=CONSTANTS.AUTH_CONFIRM_PATH_URL):
                params = {"conids": await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair),
                          "fields": CONSTANTS.MARKET_DATA_FIELDS}
                pairs_prices = await self._api_get(path_url=CONSTANTS.CONTRACT_MARKET_DATA, params=params)
                if pairs_prices and len(pairs_prices) > 0:
                    pairs_prices = pairs_prices[0]
                else:
                    self.logger().error(f"No market data for trading pair {trading_pair}")
                return pairs_prices
            else:
                raise Exception(CONSTANTS.AUTH_EXCEPTION)
        except Exception as e:
            self.logger().error(e)
            return {}

    async def ping_session(self):
        while True:
            try:
                await self._sleep(60.0)
                session_data = await self._api_get(path_url=CONSTANTS.PING_PATH_URL)
                payload = {"session": session_data.get("session")}
                auth = session_data.get("iserver").get("authStatus")
                if auth.get("authenticated") and auth.get("connected"):
                    return payload
                else:
                    raise Exception(CONSTANTS.AUTH_EXCEPTION)
            except Exception as e:
                raise Exception(e)

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            auth=self._auth)

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return IbkrAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory)

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> TradeFeeBase:
        is_maker = order_type is OrderType.LIMIT_MAKER
        return DeductedFromReturnsTradeFee(percent=self.estimate_fee_pct(is_maker))

    async def _update_trading_fees(self):
        """
        Update fees information from the exchange
        """
        pass

    async def _chunks_traded_price(self, symbol_chunk):
        exchange_info = []
        params = {
            "conids": ",".join(symbol_chunk),
            "fields": CONSTANTS.LAST_TRADED_FIELD
        }
        twice_try = 2
        while twice_try > 0:
            exchange_info = await self._api_get(path_url=CONSTANTS.CONTRACT_MARKET_DATA, params=params,
                                                limit_id=CONSTANTS.CONTRACT_MARKET_DATA)
            await self._sleep(2.0)
            twice_try -= 1
        return exchange_info

    async def get_all_last_traded_price(self, con_ids: List[str] = None):
        exchange_traded_price = {}
        try:
            if await ibkr_utils.is_authenticated_user(api_get=self._api_get, path_url=CONSTANTS.AUTH_CONFIRM_PATH_URL):
                if con_ids is None:
                    con_ids = await self._get_trading_pair_tuples()
                    con_ids = [con_id[0] for con_id in con_ids]
                con_ids_chunks = [con_ids[i:i + CONSTANTS.CHUNK_SIZE] for i in
                                  range(0, len(con_ids), CONSTANTS.CHUNK_SIZE)]
                tasks = [self._chunks_traded_price(symbol_chunk=symbol_chunk) for symbol_chunk in con_ids_chunks]
                results = await safe_gather(*tasks)
                exchange_traded_price = {d['55']: ibkr_utils.clean_traded_price(d['31']) for sublist in results
                                         for d in sublist if d.get("31")}
            else:
                self.logger().error(CONSTANTS.AUTH_EXCEPTION)
        except Exception as e:
            self.logger().error(e)
            return exchange_traded_price
        return exchange_traded_price

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        try:
            if await ibkr_utils.is_authenticated_user(api_get=self._api_get, path_url=CONSTANTS.AUTH_CONFIRM_PATH_URL):
                params = {
                    "conids": await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair),
                    "fields": CONSTANTS.MARKET_DATA_FIELDS
                }

                resp_json = await self._api_request(
                    method=RESTMethod.GET,
                    path_url=CONSTANTS.CONTRACT_MARKET_DATA,
                    params=params
                )

                return float(resp_json["31"])
            else:
                raise ValueError(CONSTANTS.AUTH_EXCEPTION)
        except Exception as e:
            self.logger().error(e)
            return float(0.0)

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return IbkrAPIUserStreamDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
        )

    async def _security_contract(self, con_id: str, order_type: str) -> Dict[str, Any]:
        order_value = True if "buy" in order_type else False
        params = {"isBuy": order_value}
        contract_rules = await self._api_get(path_url=CONSTANTS.CONTRACT_RULES.format(con_id), params=params)
        return contract_rules

    async def _place_order(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           trade_type: TradeType,
                           order_type: OrderType,
                           price: Decimal,
                           **kwargs) -> Tuple[str, float]:
        try:
            if await ibkr_utils.is_authenticated_user(api_get=self._api_get,
                                                      path_url=CONSTANTS.AUTH_CONFIRM_PATH_URL):
                account_id = await self.check_account_id_status()
                kwargs = kwargs.get("kwargs") if kwargs.get("kwargs") else kwargs
                side_str = CONSTANTS.SIDE_BUY if trade_type is TradeType.BUY else CONSTANTS.SIDE_SELL
                con_id = kwargs.get("con_id") if kwargs.get("argument") and "future" in kwargs.get("argument") else \
                    await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                sec_type = f"{con_id}@FUT" if kwargs.get("argument") and "future" in kwargs.get("argument") else f"{con_id}@STK"
                api_params = {"acctId": account_id, "conid": int(con_id),
                              "conidex": f'{con_id}@{self.ibkr_exchange_name}',
                              "orderType": "LMT" if order_type is OrderType.LIMIT else "MKT",
                              "cOID": order_id, "side": side_str, "quantity": float(amount),
                              "listingExchange": self.ibkr_exchange_name,
                              "secType": sec_type,
                              "tif": CONSTANTS.GOOD_TILL_CANCEL}
                if order_type is OrderType.LIMIT:
                    api_params["price"] = float(price)

                params = {"orders": [api_params]}
                order_result = await self._api_post(
                    path_url=CONSTANTS.ORDER_PATH_URL.format(account_id),
                    data=params, limit_id=CONSTANTS.ORDER_PATH_URL)
                reply_confirm = True
                while reply_confirm:
                    if order_result and isinstance(order_result, dict) and "error" in order_result:
                        raise ValueError(order_result.get("error"))
                    if order_result and isinstance(order_result, list):
                        order_result = order_result[0]

                    if "id" in order_result:
                        params = {"confirmed": True}
                        reply_id = order_result.get("id")
                        order_result = await self._api_post(path_url=f"{CONSTANTS.REPLY_ORDER}/{reply_id}", data=params,
                                                            limit_id=CONSTANTS.REPLY_ORDER)
                    else:
                        reply_confirm = False

                o_id = str(order_result["order_id"])
                transact_time = ibkr_utils.get_ms_timestamp()
                self._all_orders[order_id] = {"exchange_order_id": o_id, "trading_pair": trading_pair}
                order_type = "Limit" if order_type is OrderType.LIMIT else "Market"
                self.logger().info(f"{side_str.upper()} {amount} {order_type} order placed for symbol {trading_pair} "
                                   f"at {datetime.now()}")
            else:
                self._order_place = False
                raise ValueError(CONSTANTS.AUTH_EXCEPTION)
        except Exception as e:
            self._order_place = False
            raise Exception(e)
        self._order_place = False
        return o_id, transact_time

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        try:
            if await ibkr_utils.is_authenticated_user(api_get=self._api_get, path_url=CONSTANTS.AUTH_CONFIRM_PATH_URL):
                account_id = await self.check_account_id_status()
                exchange_order_id = await tracked_order.get_exchange_order_id()
                order_path = CONSTANTS.ORDER_CANCEL_URL.format(account_id) + f"/{exchange_order_id}"
                cancel_result = await self._api_delete(
                    path_url=order_path, limit_id=CONSTANTS.ORDER_CANCEL_URL)
                if cancel_result.get("msg") and cancel_result.get("msg") == CONSTANTS.CANCELLED_ORDER_MESSAGE:
                    if tracked_order.client_order_id in self._all_orders:
                        del self._all_orders[tracked_order.client_order_id]
                    return True
                return False
            else:
                raise ValueError(CONSTANTS.AUTH_EXCEPTION)
        except Exception as e:
            self.logger().exception(e)
            return False

    @staticmethod
    def _get_path_url(domain_url: str, path_url: str):
        return domain_url + path_url
    
    async def _option_chain_price(self, trading_pair: str) -> Dict[str, Any]:
        semaphore = asyncio.Semaphore(10)
        timeout = aiohttp.ClientTimeout(total=20)
        option_chains_list = {}

        # Define parameters for the search
        trading_pair = trading_pair
        params = {"symbol": trading_pair}
        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                async with semaphore:
                    # Send an initial request to the main site to retrieve cookies
                    async with session.get(CONSTANTS.NSE_DOMAIN, headers=CONSTANTS.NSE_HEADERS) as main_response:
                        if main_response.status != 200:
                            self.logger().error(f"Initial request failed with status code: {main_response.status}")
                            return {}
                        else:
                            self.logger().info("Initial request successful, cookies retrieved.")
                            # Wait for the rate limit duration before proceeding
                            await asyncio.sleep(1/10)

                    # Now send the actual request using the same session object
                    async with session.get(CONSTANTS.NSE_OPTION_CHAIN, headers=CONSTANTS.NSE_HEADERS, params=params) as response:
                        if response.status != 200:
                            self.logger().error(f"Request to option chain API failed with status code: {response.status}")
                            return {}

                        option_chains = await response.json()

                        if option_chains is None or not isinstance(option_chains, dict) or len(option_chains.get("records", {}).get("data", [])) == 0:
                            self.logger().error(f"No option chain data found for symbol {trading_pair}",
                                                exc_info=True)
                            return {}

                    # Wait for the rate limit duration before proceeding
                    option_chains_list = {trading_pair: option_chains}
                    return option_chains_list
            except Exception as e:
                # Handle exceptions and print the error message
                self.logger().error(e)
                return option_chains_list

    @staticmethod
    def get_missing_symbols(stock_list, transformed_data):
        return [symbol for symbol in stock_list if symbol not in transformed_data]

    async def fetch_option_chains(self, stock_list: List[str]) -> Dict[str, Any]:
        transformed_data = {}
        if stock_list:
            trading_pair_list = stock_list
            chunk_size = 10
            trading_pair_chunks = [trading_pair_list[i:i + chunk_size] for i in
                                   range(0, len(trading_pair_list), chunk_size)]
            for chunk in trading_pair_chunks:
                tasks = [self._option_chain_price(trading_pair) for trading_pair in chunk]
                results = await safe_gather(*tasks)
                transformed_data.update({k: v for d in results for k, v in d.items()})

                # Get missing symbols
                missing_symbols = self.get_missing_symbols(chunk, transformed_data)

                if missing_symbols:
                    # Try fetching the missing symbols once more
                    await self._sleep(2.0)
                    tasks = [self._option_chain_price(trading_pair) for trading_pair in missing_symbols]
                    results = await safe_gather(*tasks)
                    transformed_data.update({k: v for d in results for k, v in d.items()})
                await self._sleep(1.0)

        return transformed_data

    async def options_contracts(self) -> Dict[str, Any]:
        if self._options_data:
            return self._options_data
        else:
            self._options_data = await self.fetch_option_chains()
            return self._options_data

    async def fetch_sec_type(self, con_ids):
        con_id_dict = {}
        params = {"conids": ",".join(con_ids)}
        result = await self._api_get(path_url=CONSTANTS.CONTRACT_RULES, params=params,
                                     limit_id=CONSTANTS.CONTRACT_RULES)
        for item in result.get("secdef", []):
            if item.get("listingExchange", "").upper() == self.ibkr_exchange_name.upper():
                con_id_dict[item.get("ticker")] = item.get("conid")
        return con_id_dict

    async def _request_futures_symbol_contracts(self, trading_pair: List[Tuple[str, str]],
                                                expiry_date: str = None) -> Dict[str, Any]:
        """
        Fetch future contracts for the given trading pairs.

        Args:
            trading_pair (Tuple[str, str]): A tuple of trading pairs.

        Returns:
            Dict[str, Any]: A dictionary mapping trading pairs to their corresponding future contracts.
        """
        future_contract = {}
        try:
            # Extract the symbols from the trading pairs
            symbols = [symbol[1] for symbol in trading_pair]
            # Create request parameters
            params = {"symbols": ",".join(symbols)}
            # Fetch future contracts from the API
            result = await self._api_get(path_url=CONSTANTS.FUTURE_CONTRACT_PATH_URL, params=params,
                                         limit_id=CONSTANTS.FUTURE_CONTRACT_PATH_URL)
            for con_id, symbol in trading_pair:
                contracts = []
                base = symbol
                # Filter contracts by the underlyingConid
                for item in result.get(base, []):
                    if str(item.get("underlyingConid")) == con_id:
                        item["underlyingConid"] = str(item["underlyingConid"])
                        item["conid"] = str(item["conid"])
                        item["symbol"] = symbol
                        item["expirationDate"] = ibkr_utils.convert_str_to_date(
                            str(item["expirationDate"]), '%Y%m%d')
                        item["expirationDate"] = item["expirationDate"].strftime("%d-%b-%Y")
                        if expiry_date:
                            if item["expirationDate"] == expiry_date:
                                contracts.append(item)
                        else:
                            contracts.append(item)
                future_contract[symbol] = contracts
            return future_contract
        except Exception as e:
            self.logger().error(e)
            return future_contract

    async def fetch_future_symbol_contracts(self, expiry_date: str = None) -> Dict[str, Any]:
        """
        Fetch future contracts for all trading pairs in chunks of 50.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries containing future contracts for each chunk of trading pairs.
        """
        contracts = {}
        try:
            # Check if the user is authenticated
            if await ibkr_utils.is_authenticated_user(api_get=self._api_get, path_url=CONSTANTS.AUTH_CONFIRM_PATH_URL):
                if self._trading_pairs:
                    trading_pair_tuples = await self._get_trading_pair_tuples()
                    # Split the trading pairs into chunks of 10
                    chunk_size = 50
                    symbol_chunks = [trading_pair_tuples[i:i + chunk_size] for i in
                                     range(0, len(trading_pair_tuples), chunk_size)]
                    # Create tasks for fetching future contracts for each chunk
                    tasks = [self._request_futures_symbol_contracts(trading_pair, expiry_date) for trading_pair in symbol_chunks]
                    # Execute the tasks concurrently
                    results = await safe_gather(*tasks)

                    if results and isinstance(results, list):
                        results = {k: v for data in results for k, v in data.items() if isinstance(data, dict)}
                        all_con_ids = [item["conid"] for items in results.values() for item in items]
                        contracts.update(await self.fetch_sec_type(all_con_ids))
                    elif results:
                        all_con_ids = [item["conid"] for items in results.values() for item in items]
                        contracts.update(await self.fetch_sec_type(all_con_ids))
            else:
                self.logger().error(CONSTANTS.AUTH_EXCEPTION)
            return contracts
        except Exception as e:
            self.logger().error(e)
            return contracts

    async def future_contracts(self) -> Dict[str, Any]:
        if self._futures_data:
            return self._futures_data
        else:
            self._futures_data = await self.fetch_future_symbol_contracts()
            return self._futures_data

    async def _update_trading_rules(self):
        exchange_info = {}
        trading_rules_list = await self._format_trading_rules(exchange_info)
        self._trading_rules.clear()
        for trading_rule in trading_rules_list:
            self._trading_rules[trading_rule.trading_pair] = trading_rule

    async def _status_polling_loop_fetch_updates(self):
        await self._update_order_fills_from_trades()
        await super()._status_polling_loop_fetch_updates()

    async def _trades_data(self):
        trades_data = []
        try:
            if await ibkr_utils.is_authenticated_user(api_get=self._api_get, path_url=CONSTANTS.AUTH_CONFIRM_PATH_URL):
                trades_data = await self._api_get(path_url=CONSTANTS.MY_TRADES_PATH_URL,
                                                  limit_id=CONSTANTS.MY_TRADES_PATH_URL)
                if trades_data and len(trades_data) > 0:
                    return trades_data
                return await self._api_get(path_url=CONSTANTS.MY_TRADES_PATH_URL,
                                           limit_id=CONSTANTS.MY_TRADES_PATH_URL)
            else:
                self.logger().error(CONSTANTS.AUTH_EXCEPTION)
                return trades_data
        except Exception as e:
            self.logger().error(e)
            return trades_data

    async def _update_order_fills_from_trades(self):
        """
        This is intended to be a backup measure to get filled events with trade ID for orders,
        in case ibkr user stream events are not working.
        NOTE: It is not required to copy this functionality in other connectors.
        This is separated from _update_order_status which only updates the order status without producing filled
        events, since ibkr get order endpoint does not return trade IDs and vice versa.
        """
        try:
            small_interval_last_tick = self._last_poll_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL
            small_interval_current_tick = self.current_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL
            long_interval_last_tick = self._last_poll_timestamp / self.LONG_POLL_INTERVAL
            long_interval_current_tick = self.current_timestamp / self.LONG_POLL_INTERVAL

            if (long_interval_current_tick > long_interval_last_tick
                    or (self.in_flight_orders and small_interval_current_tick > small_interval_last_tick)):
                if await ibkr_utils.is_authenticated_user(api_get=self._api_get,
                                                          path_url=CONSTANTS.AUTH_CONFIRM_PATH_URL):
                    order_by_client_order_id_map = {order.client_order_id: order for order in
                                                    self._order_tracker.all_fillable_orders.values()}
                    trade_by_exchange_trade_id_map = {trade.exchange_trade_id: trade for trade in self._current_trade_fills
                                                      if trade.market == self.display_name}
                    if len(order_by_client_order_id_map) == 0 and len(trade_by_exchange_trade_id_map) == 0:
                        return
                    trades_data = await self._trades_data()

                    if isinstance(trades_data, list) and len(trades_data) == 0:
                        self.logger().network(
                            f"Error fetching trades update for the order"
                        )
                        return
                    for trade in trades_data:
                        client_order_id = trade.get("order_ref", "")
                        exchange_order_id = self._all_orders.get(client_order_id, {}).get("exchange_order_id", "")
                        if client_order_id and client_order_id in order_by_client_order_id_map:
                            tracked_order = order_by_client_order_id_map[client_order_id]

                            if tracked_order.exchange_order_id in self._orders_list:
                                order_data = self._orders_list[tracked_order.exchange_order_id]
                            else:
                                order_data = await self._api_get(
                                    path_url=CONSTANTS.ORDER_STATUS_PATH_URL.format(tracked_order.exchange_order_id),
                                    limit_id=CONSTANTS.ORDER_STATUS_PATH_URL)
                                self._orders_list[tracked_order.exchange_order_id] = order_data

                            if str(order_data["conid"]) != str(trade["conid"]):
                                continue

                            # This is a fill for a tracked order
                            fee = TradeFeeBase.new_spot_fee(
                                fee_schema=self.trade_fee_schema(),
                                trade_type=tracked_order.trade_type,
                                percent_token=None,
                                flat_fees=[]
                            )
                            trade_update = TradeUpdate(
                                trade_id=trade["execution_id"],
                                client_order_id=tracked_order.client_order_id,
                                exchange_order_id=tracked_order.exchange_order_id,
                                trading_pair=tracked_order.trading_pair,
                                fee=fee,
                                fill_base_amount=Decimal(trade["size"]),
                                fill_quote_amount=Decimal("0.0"),
                                fill_price=Decimal(trade["price"]),
                                fill_timestamp=trade["trade_time_r"] * 1e-3,
                            )
                            self._order_tracker.process_trade_update(trade_update)
                        elif (client_order_id is not None and exchange_order_id is not None
                              and trade["execution_id"] not in trade_by_exchange_trade_id_map and
                              exchange_order_id in set(self._exchange_order_ids.keys())):
                            # This is a fill of an order registered in the DB but not tracked any more
                            trading_pair = self._all_orders[client_order_id].get("trading_pair")
                            if exchange_order_id in self._orders_list:
                                order_data = self._orders_list[exchange_order_id]
                            else:
                                order_data = await self._api_get(
                                    path_url=CONSTANTS.ORDER_STATUS_PATH_URL.format(exchange_order_id),
                                    limit_id=CONSTANTS.ORDER_STATUS_PATH_URL)
                                self._orders_list[exchange_order_id] = order_data

                            if str(order_data["conid"]) != str(trade["conid"]):
                                continue

                            self._current_trade_fills.add(TradeFillOrderDetails(
                                market=self.display_name,
                                exchange_trade_id=trade["execution_id"],
                                symbol=trading_pair))
                            self.trigger_event(
                                MarketEvent.OrderFilled,
                                OrderFilledEvent(
                                    timestamp=trade["trade_time_r"] * 1e-3,
                                    order_id=exchange_order_id,
                                    trading_pair=trading_pair,
                                    trade_type=TradeType.BUY if "B" in trade["side"] else TradeType.SELL,
                                    order_type=OrderType.MARKET if "MARKET" in order_data["order_type"] else OrderType.LIMIT,
                                    price=Decimal(trade["price"]),
                                    amount=Decimal(trade["size"]),
                                    trade_fee=DeductedFromReturnsTradeFee(
                                        flat_fees=[]
                                    ),
                                    exchange_trade_id=trade["execution_id"]
                                ))
                            self.logger().info(f"Recreating missing trade in TradeFill: {trade}")
                else:
                    self.logger().error(CONSTANTS.AUTH_EXCEPTION)
        except Exception as e:
            self.logger().error(e)

    async def _user_stream_event_listener(self):
        pass

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        """
            Trading Rules Here are set to default Value
        """
        retval = []
        try:
            trading_pairs = await self.all_trading_pairs()
            for trading_pair in trading_pairs:
                try:
                    retval.append(
                        TradingRule(trading_pair=trading_pair, min_order_size=1, min_order_value=1))
                except Exception:
                    self.logger().exception(f"Error parsing the trading pair rule {trading_pair}. Skipping.")
        except Exception as e:
            self.logger().error(e)
        finally:
            return retval

    async def _all_trades_updates_for_orders(self, order: InFlightOrder, trades_data: List[Dict[str, Any]]) -> List[TradeUpdate]:
        trade_updates = []
        try:
            exchange_order_id = await order.get_exchange_order_id()
            if exchange_order_id is not None:
                client_order_id = order.client_order_id
                trading_pair = order.trading_pair

                for trade in trades_data:
                    if client_order_id == trade.get("order_ref", ""):
                        fee = TradeFeeBase.new_spot_fee(
                            fee_schema=self.trade_fee_schema(),
                            trade_type=order.trade_type,
                            percent_token=None,
                            flat_fees=[]
                        )
                        trade_update = TradeUpdate(
                            trade_id=trade["execution_id"],
                            client_order_id=client_order_id,
                            exchange_order_id=exchange_order_id,
                            trading_pair=trading_pair,
                            fee=fee,
                            fill_base_amount=Decimal(trade["size"]),
                            fill_quote_amount=Decimal("0.0"),
                            fill_price=Decimal(trade["price"]),
                            fill_timestamp=trade["trade_time_r"] * 1e-3,
                        )
                        trade_updates.append(trade_update)
                        break
            return trade_updates
        except Exception as e:
            self.logger().error(e)
            return trade_updates

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        pass

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        try:
            exchange_order_id = await tracked_order.get_exchange_order_id()
            updated_order_data = await self._api_get(
                path_url=CONSTANTS.ORDER_STATUS_PATH_URL.format(exchange_order_id),
                limit_id=CONSTANTS.ORDER_STATUS_PATH_URL)

            new_state = CONSTANTS.ORDER_STATE[updated_order_data["order_status"]]
            order_time = datetime.strptime(updated_order_data["order_time"], "%y%m%d%H%M%S").timestamp()

            order_update = OrderUpdate(
                client_order_id=tracked_order.client_order_id,
                exchange_order_id=str(updated_order_data["order_id"]),
                trading_pair=tracked_order.trading_pair,
                update_timestamp=int(order_time) * 1e-3,
                new_state=new_state,
            )

            return order_update
        except Exception as e:
            self.logger().error(f"Exception occurred while order status check due to {e}")
            raise

    async def _update_balances(self):
        try:
            if await ibkr_utils.is_authenticated_user(api_get=self._api_get, path_url=CONSTANTS.AUTH_CONFIRM_PATH_URL):
                account_id = await self.check_account_id_status()
                local_asset_names = set(self._account_balances.keys())
                remote_asset_names = set()

                account_info = await self._api_get(
                    path_url=CONSTANTS.BALANCE_PATH_URL.format(account_id), limit_id=CONSTANTS.BALANCE_PATH_URL
                    )
                for currency, data in account_info.items():
                    asset_name = currency
                    free_balance = Decimal(data["cashbalance"])
                    total_balance = Decimal(data["cashbalance"])
                    self._account_available_balances[asset_name] = free_balance
                    self._account_balances[asset_name] = total_balance
                    remote_asset_names.add(asset_name)

                asset_names_to_remove = local_asset_names.difference(remote_asset_names)
                for asset_name in asset_names_to_remove:
                    del self._account_available_balances[asset_name]
                    del self._account_balances[asset_name]
            else:
                raise ValueError(CONSTANTS.AUTH_EXCEPTION)
        except Exception as e:
            raise ValueError(f"Error occur while fetching balance data due to {e}")

    async def _update_orders_with_error_handler(self, orders: List[InFlightOrder], error_handler: Callable):
        if len(orders) > 0:
            try:
                if await ibkr_utils.is_authenticated_user(api_get=self._api_get, path_url=CONSTANTS.AUTH_CONFIRM_PATH_URL):
                    for order in orders:
                        try:
                            order_update = await self._request_order_status(tracked_order=order)
                            self._order_tracker.process_order_update(order_update)
                            await self._sleep(1.0)
                        except asyncio.CancelledError:
                            raise
                        except Exception as request_error:
                            await error_handler(order, request_error)
                else:
                    raise ValueError(CONSTANTS.AUTH_EXCEPTION)
            except Exception as e:
                raise Exception(e)

    async def _update_orders_fills(self, orders: List[InFlightOrder]):
        if len(orders) > 0:
            trades_data = await self._trades_data()
            if trades_data:
                for order in orders:
                    try:
                        trade_updates = await self._all_trades_updates_for_orders(order=order, trades_data=trades_data)
                        for trade_update in trade_updates:
                            self._order_tracker.process_trade_update(trade_update)
                    except asyncio.CancelledError:
                        raise
                    except Exception as request_error:
                        self.logger().warning(
                            f"Failed to fetch trade updates for order {order.client_order_id}. Error: {request_error}",
                            exc_info=request_error,
                        )

    async def _create_order(self,
                            trade_type: TradeType,
                            order_id: str,
                            trading_pair: str,
                            amount: Decimal,
                            order_type: OrderType,
                            price: Optional[Decimal] = None,
                            **kwargs):
        """
        Creates an order in the exchange using the parameters to configure it

        :param trade_type: the side of the order (BUY of SELL)
        :param order_id: the id that should be assigned to the order (the client id)
        :param trading_pair: the token pair to operate with
        :param amount: the order amount
        :param order_type: the type of order to create (MARKET, LIMIT, LIMIT_MAKER)
        :param price: the order price
        """
        trading_rule = self._trading_rules[trading_pair]

        self.start_tracking_order(
            order_id=order_id,
            exchange_order_id=None,
            trading_pair=trading_pair,
            order_type=order_type,
            trade_type=trade_type,
            price=price,
            amount=amount,
            **kwargs,
        )
        order = self._order_tracker.active_orders[order_id]
        current_time = datetime.now(timezone(timedelta(hours=5.5)))
        if not price or price.is_nan() or price == s_decimal_0 and order_type in [OrderType.LIMIT]:
            self.logger().warning(f"{trade_type.name.title()} order value {price} is lower than the minimum order "
                                  f"value {trading_rule.min_order_value}. The order will not be created, increase the "
                                  f"amount to be higher than the minimum order value.")
            self._update_order_after_failure(order_id=order_id, trading_pair=trading_pair)
            return

        if order_type not in self.supported_order_types():
            self.logger().error(f"{order_type} is not in the list of supported order types")
            self._update_order_after_failure(order_id=order_id, trading_pair=trading_pair)
            raise

        elif amount < trading_rule.min_order_size:
            self.logger().warning(f"{trade_type.name.title()} order amount {amount} is lower than the minimum order "
                                  f"size {trading_rule.min_order_size}. The order will not be created, increase the "
                                  f"amount to be higher than the minimum order size.")
            self._update_order_after_failure(order_id=order_id, trading_pair=trading_pair)
            raise

        elif CONSTANTS.START_TIME > current_time or current_time >= CONSTANTS.END_TIME:
            self.logger().warning(f"IBKR {self.ibkr_exchange_name} Exchange trade time is over")
            raise
        try:
            while self._order_place:
                await self._sleep(5.0)
            self._order_place = True
            await self._sleep(5.0)
            await self._place_order_and_process_update(order=order, **kwargs,)

        except asyncio.CancelledError:
            raise
        except Exception as ex:
            self._on_order_failure(
                order_id=order_id,
                trading_pair=trading_pair,
                amount=amount,
                trade_type=trade_type,
                order_type=order_type,
                price=price,
                exception=ex,
                **kwargs,
            )

    def _on_order_failure(
        self,
        order_id: str,
        trading_pair: str,
        amount: Decimal,
        trade_type: TradeType,
        order_type: OrderType,
        price: Optional[Decimal],
        exception: Exception,
        **kwargs,
    ):
        self.logger().network(
            f"Error submitting {trade_type.name.lower()} {order_type.name.upper()} order to {self.name_cap} for "
            f"{amount} {trading_pair} {price}.",
            exc_info=True,
            app_warning_msg=str(exception)
        )
        self._update_order_after_failure(order_id=order_id, trading_pair=trading_pair)
