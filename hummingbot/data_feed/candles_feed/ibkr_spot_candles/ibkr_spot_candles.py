import asyncio
import logging
from typing import Any, Dict, Optional
import json
from datetime import datetime, timezone

import numpy as np

from hummingbot.core.network_iterator import NetworkStatus, safe_ensure_future
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest, WSPlainTextRequest
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.data_feed.candles_feed.ibkr_spot_candles import constants as CONSTANTS
from hummingbot.data_feed.candles_feed.candles_base import CandlesBase
from hummingbot.logger import HummingbotLogger


class IbkrSpotCandles(CandlesBase):
    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, trading_pair: str, interval: str = "1m", max_records: int = 150, exchange: str = "NSE"):
        self.exchange = exchange
        self._con_id = None
        self._server_ids: set = set()
        self._timestamp = None
        self.first_call = True
        super().__init__(trading_pair, interval, max_records)

    @property
    def name(self):
        return f"ibkr_{self._trading_pair}"

    @property
    def timestamp(self):
        return self._timestamp

    @property
    def rest_url(self):
        return CONSTANTS.REST_URL

    @property
    def wss_url(self):
        return CONSTANTS.WSS_URL

    @property
    def health_check_url(self):
        return self.rest_url + CONSTANTS.HEALTH_CHECK_ENDPOINT

    @property
    def candles_url(self):
        return self.rest_url + CONSTANTS.CANDLES_ENDPOINT

    @property
    def rate_limits(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def intervals(self):
        return CONSTANTS.INTERVALS
    
    def get_exchange_trading_pair(self, trading_pair):
        return self._con_id

    def get_exchange_url(self):
        return self.rest_url + CONSTANTS.EXCHANGE_URL

    async def _ping_session(self):
        rest_assistant = await self._api_factory.get_rest_assistant()
        try:
            session_data = await rest_assistant.execute_request(url=self.health_check_url,
                                                                throttler_limit_id=CONSTANTS.HEALTH_CHECK_ENDPOINT)
            return session_data
        except Exception as e:
            raise Exception(e)

    async def keep_session_socket_alive(self, ws: WSAssistant):
        while True:
            payload = "tick"
            subscribe_trade_request: WSPlainTextRequest = WSPlainTextRequest(payload=payload)
            await ws.send(subscribe_trade_request)
            await self._sleep(120.0)  # needs to call this function after 120 seconds
    
    async def get_contracts(self, trading_pair: str) -> Optional[str]:
        try:
            params = {"exchange": self.exchange}
            symbol = trading_pair
            rest_assistant = await self._api_factory.get_rest_assistant()
            exchange = await rest_assistant.execute_request(url=self.get_exchange_url(),
                                                            throttler_limit_id=CONSTANTS.CANDLES_ENDPOINT,
                                                            params=params)
            con_id = [item.get("conid") for item in exchange if item.get("ticker", "").upper() == symbol.upper()]
            if con_id:
                return str(con_id[0])
            else:
                raise ValueError("No contract found for given trading pair")
        except Exception as e:
            self.logger().exception(e)
            raise ValueError("Please make sure your ibkr session is running and authenticated")

    async def listen_for_subscriptions(self):
        """
        Connects to the candlestick websocket endpoint and listens to the messages sent by the
        exchange.
        """
        await self._ping_session()
        self._con_id = await self.get_contracts(self._trading_pair)
        ws: Optional[WSAssistant] = None
        while True:
            try:
                ws: WSAssistant = await self._connected_websocket_assistant()
                await self._sleep(5.0)
                asyncio.create_task(self.keep_session_socket_alive(ws=ws))
                await self._subscribe_channels(ws)
                await self._process_websocket_messages(websocket_assistant=ws)
            except asyncio.CancelledError:
                raise
            except ConnectionError as connection_exception:
                self.logger().warning(f"The websocket connection was closed ({connection_exception})")
            except Exception:
                self.logger().exception(
                    "Unexpected error occurred when listening to historical data. Retrying in 1 seconds...",
                )
                await self._sleep(1.0)
            finally:
                await self._unsubscribe_channels(ws=ws)
                await self._on_order_stream_interruption(websocket_assistant=ws)

    async def _connected_websocket_assistant(self) -> WSAssistant:
        try:
            session_data = await self._ping_session()
            payload = {"session": session_data.get("session")}
        except Exception as e:
            raise Exception(e)
        try:
            auth = session_data.get("iserver").get("authStatus")
            if auth.get("authenticated") and auth.get("connected"):
                ws: WSAssistant = await self._api_factory.get_ws_assistant()
                await ws.connect(ws_url=self.wss_url, ping_timeout=30, message_timeout=30)
                subscribe_candles_request: WSJSONRequest = WSJSONRequest(payload=payload)
                await ws.send(subscribe_candles_request)
                return ws
            else:
                raise ValueError(CONSTANTS.AUTH_EXCEPTION)
        except asyncio.CancelledError:
            raise
        except ConnectionError as connection_exception:
            raise ConnectionError(f"The websocket connection was closed ({connection_exception})")
        except Exception as e:
            raise ValueError("Unexpected error occurred when listening to historical data. Retrying in 1 seconds...")

    async def check_network(self) -> NetworkStatus:
        rest_assistant = await self._api_factory.get_rest_assistant()
        await rest_assistant.execute_request(url=self.health_check_url,
                                             throttler_limit_id=CONSTANTS.HEALTH_CHECK_ENDPOINT)
        return NetworkStatus.CONNECTED

    @staticmethod
    def add_interval_to_timestamp(timestamp_ms, interval):
        # Convert timestamp from milliseconds to seconds
        timestamp_sec = timestamp_ms / 1000
    
        # Convert timestamp to UTC datetime object
        utc_datetime = datetime.fromtimestamp(timestamp_sec, tz=timezone.utc)
        
        # Add interval to UTC datetime
        new_datetime = utc_datetime + CONSTANTS.INTERVAL_MAPPING[interval] + CONSTANTS.INTERVAL_MAPPING[interval]
    
        # Format the new datetime as "yyyymmdd-HH:MM:SS"
        formatted_datetime = new_datetime.strftime("%Y%m%d-%H:%M:%S")
        
        return formatted_datetime

    @staticmethod
    def find_period_value(max_len: int, interval: str):
        """
            There is no limit functionality in Ibkr. The maximum points return by Ibkr historical data is 1000
            In order to reduce response time we need to calculate what minimum period value should we set as param
        """
        if interval in ["1min", "2min", "3min", "5min", "10min", "15min", "30min"]:
            if max_len >= 1000:
                return CONSTANTS.INTERVALS_TO_PERIOD[interval]
            min_in_trading_day = 60 * 6
            interval_min_in_trading_day = (min_in_trading_day // int(interval.replace("min", ""))) + 1
            total_days = (max_len // interval_min_in_trading_day) + 1
            total_days = 1000 if total_days > 1000 else total_days
            return f"{total_days}d"

        if interval in ["1h", "2h", "3h", "4h", "8h", "15min", "30min"]:
            if max_len >= 1000:
                return CONSTANTS.INTERVALS_TO_PERIOD[interval]
            hours_in_trading_day = 6 * 5
            interval_hours_in_trading_day = (hours_in_trading_day // int(interval.replace("h", ""))) + 1
            total_weeks = (max_len // interval_hours_in_trading_day) + 1
            total_weeks = 792 if total_weeks > 792 else total_weeks
            return f"{total_weeks}w"

        if interval in ["1d"]:
            if max_len >= 1000:
                return CONSTANTS.INTERVALS_TO_PERIOD[interval]
            days_in_trading_month = 20
            interval_days_in_trading_month = (days_in_trading_month // int(interval.replace("d", "")))
            total_months = (max_len // interval_days_in_trading_month) + 1
            total_months = 182 if total_months > 182 else total_months
            return f"{total_months}m"

        if interval in ["1w"]:
            if max_len >= 728:
                return CONSTANTS.INTERVALS_TO_PERIOD[interval]
            weeks_in_trading_year = 52
            interval_weeks_in_trading_year = (weeks_in_trading_year // int(interval.replace("w", "")))
            total_years = (max_len // interval_weeks_in_trading_year) + 1
            total_years = 14 if total_years > 14 else total_years
            return f"{total_years}y"

        if interval in ["1m"]:
            if max_len >= 180:
                return CONSTANTS.INTERVALS_TO_PERIOD[interval]
            months_in_trading_year = 12
            interval_months_in_trading_year = (months_in_trading_year // int(interval.replace("m", "")))
            total_years = (max_len // interval_months_in_trading_year) + 1
            total_years = 15 if total_years > 15 else total_years
            return f"{total_years}y"

    @staticmethod
    def _get_path_url(domain_url: str, path_url: str):
        return domain_url + path_url

    async def fetch_candles_interval(self):
        try:
            rest_assistant = await self._api_factory.get_rest_assistant()
            interval = CONSTANTS.INTERVALS[self.interval]
            params = dict(conid=self._con_id, bar=interval, exchange=self.exchange,
                          period=CONSTANTS.INTERVAL_DEFAULT_PERIOD[interval], outsideRth="true")

            # params["startTime"] = self.add_interval_to_timestamp(self.timestamp, interval)
            candles = await rest_assistant.execute_request(url=self.candles_url,
                                                           throttler_limit_id=CONSTANTS.CANDLES_ENDPOINT,
                                                           params=params)
            for i in candles.get("data", []):
                timestamp_ms = i["t"]
                if timestamp_ms < self.timestamp:
                    continue
                open = i["o"]
                high = i["h"]
                low = i["l"]
                close = i["c"]
                volume = i["v"]

                # no data field
                quote_asset_volume = 0
                n_trades = 0
                taker_buy_base_volume = 0
                taker_buy_quote_volume = 0
                if timestamp_ms == self.timestamp:
                    self._candles.pop()
                    self._candles.append(np.array([timestamp_ms, open, high, low, close, volume,
                                                   quote_asset_volume, n_trades, taker_buy_base_volume,
                                                   taker_buy_quote_volume]))
                if timestamp_ms > self.timestamp:
                    self._candles.append(np.array([timestamp_ms, open, high, low, close, volume,
                                                   quote_asset_volume, n_trades, taker_buy_base_volume,
                                                   taker_buy_quote_volume]))
                    self._timestamp = timestamp_ms
        except ConnectionError as connection_exception:
            self.logger().error(f"The network connection was closed ({connection_exception})")
        except Exception as e:
            self.logger().error(f"Stream Error for symbol {self._trading_pair}")

    async def fetch_candles(self,
                            start_time: Optional[int] = None,
                            end_time: Optional[int] = None,
                            limit: Optional[int] = 1000):
        new_hb_candles = []
        try:
            rest_assistant = await self._api_factory.get_rest_assistant()
            interval = CONSTANTS.INTERVALS[self.interval]
            params = dict(conid=self._con_id, bar=interval, exchange=self.exchange,
                          period=self.find_period_value(max_len=limit, interval=interval), outsideRth="true")
            if end_time:
                params["startTime"] = self.add_interval_to_timestamp(end_time, interval)
            candles = await rest_assistant.execute_request(url=self.candles_url,
                                                           throttler_limit_id=CONSTANTS.CANDLES_ENDPOINT,
                                                           params=params)
            for i in candles.get("data", [])[::-1]:
                if limit == 0:
                    break
                timestamp_ms = i["t"]
                if timestamp_ms >= self.timestamp:
                    continue
                open = i["o"]
                high = i["h"]
                low = i["l"]
                close = i["c"]
                volume = i["v"]

                # no data field
                quote_asset_volume = 0
                n_trades = 0
                taker_buy_base_volume = 0
                taker_buy_quote_volume = 0
                new_hb_candles.append([timestamp_ms, open, high, low, close, volume,
                                       quote_asset_volume, n_trades, taker_buy_base_volume,
                                       taker_buy_quote_volume])
                limit -= 1
            return np.array(new_hb_candles).astype(float)
        except ConnectionError as connection_exception:
            self.logger().error(f"The network connection was closed ({connection_exception})")
        except Exception as e:
            self.logger().error(f"No historical data found for symbol {self._trading_pair}")

    async def fill_historical_candles(self):
        
        interval = CONSTANTS.INTERVALS[self.interval]
        denom = 999 if interval not in ["1w", "1m"] else 779 if interval == "1w" else 179
        max_request_needed = (self._candles.maxlen // denom) + 1
        requests_executed = 0
        while not self.ready:
            missing_records = self._candles.maxlen - len(self._candles)
            end_timestamp = int(self._candles[-1][0])
            try:
                if requests_executed < max_request_needed:
                    # we have to add one more since, the last row is not going to be included
                    candles = await self.fetch_candles(end_time=end_timestamp, limit=missing_records + 1)
                    # we are computing again the quantity of records again since the websocket process is able to
                    # modify the deque and if we extend it, the new observations are going to be dropped.
                    if candles is not None and len(candles) > 0:
                        missing_records = self._candles.maxlen - len(self._candles)
                        self._candles.extendleft(candles[-(missing_records + 1):-1][::-1])
                        print("Candles Data:", self._candles)
                    requests_executed += 1
                else:
                    self.logger().error(f"There is no data available for the quantity of "
                                        f"candles requested for {self.name}.")
                    raise
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception(
                    "Unexpected error occurred when getting historical klines. Retrying in 1 seconds...",
                )
                await self._sleep(1.0)

    async def _unsubscribe_channels(self, ws: WSAssistant):
        for element in self._server_ids.copy():
            try:
                payload = f"umh+{element}"
                unsubscribe_candles_request: WSPlainTextRequest = WSPlainTextRequest(payload=payload)
                await ws.send(unsubscribe_candles_request)
                self._server_ids.remove(element)
                self.logger().info("Unsubscribed to historical data...")
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    "Unexpected error occurred unsubscribing to historical data...",
                    exc_info=True
                )
                raise

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribes to the candles events through the provided websocket connection.
        :param ws: the websocket assistant used to connect to the exchange
        """
        try:
            interval = CONSTANTS.INTERVALS[self.interval]
            params = json.dumps({"exchange": self.exchange, "period": CONSTANTS.INTERVAL_DEFAULT_PERIOD[interval],
                                 "bar": interval, "source": "trades", "outsideRth": True,
                                 "format": "%o/%c/%h/%l/%v/%t/%b"})
            payload = f"smh+{self._con_id}+{params}"
            subscribe_candles_request: WSPlainTextRequest = WSPlainTextRequest(payload=payload)
            await ws.send(subscribe_candles_request)
            self.logger().info("Subscribed to historical data...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to historical data...",
                exc_info=True
            )
            raise

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant):
        async for ws_response in websocket_assistant.iter_messages():
            data: Dict[str, Any] = json.loads(ws_response.data.decode("UTF-8"))
            if data is not None and data.get("topic") == f"smh+{self._con_id}":  # data will be None when the websocket is disconnected
                if data.get("serverId"):
                    self._server_ids.add(data.get("serverId"))
                if self.first_call:
                    self.first_call = False
                    for item in data.get("data", [])[::-1]:
                        self._timestamp = item.get("t")
                        timestamp = item.get("t")
                        open = item.get("o")
                        high = item.get("h")
                        low = item.get("l")
                        close = item.get("c")
                        volume = item.get("v")
                        quote_asset_volume = 0
                        n_trades = 0
                        taker_buy_base_volume = 0
                        taker_buy_quote_volume = 0
                        if len(self._candles) == 0:
                            self._candles.append(np.array([timestamp, open, high, low, close, volume,
                                                           quote_asset_volume, n_trades, taker_buy_base_volume,
                                                           taker_buy_quote_volume]))
                            safe_ensure_future(self.fill_historical_candles())
                        break
                else:
                    for item in data.get("data", [])[::-1]:
                        if item.get("t"):
                            if self.ready:
                                await self.fetch_candles_interval()
                        break
