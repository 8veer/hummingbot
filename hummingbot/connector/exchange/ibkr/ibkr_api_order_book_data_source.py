import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional
import json

from hummingbot.connector.exchange.ibkr import ibkr_constants as CONSTANTS, ibkr_web_utils as web_utils
from hummingbot.connector.exchange.ibkr.ibkr_order_book import IbkrOrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest, WSPlainTextRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger
from hummingbot.core.utils.async_utils import safe_ensure_future

if TYPE_CHECKING:
    from hummingbot.connector.exchange.ibkr.ibkr_exchange import IbkrExchange

from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.client.config.client_config_map import ClientConfigMap


class IbkrAPIOrderBookDataSource(OrderBookTrackerDataSource):
    def __init__(self,
                 trading_pairs: List[str],
                 connector: 'IbkrExchange',
                 api_factory: WebAssistantsFactory):
        super().__init__(trading_pairs)
        self._connector = connector
        self._trade_messages_queue_key = CONSTANTS.TRADE_EVENT_TYPE
        self._diff_messages_queue_key = CONSTANTS.DIFF_EVENT_TYPE
        self._api_factory = api_factory
        self.stop_event: asyncio.Event = asyncio.Event()

    @staticmethod
    def path_url(base_url: str, relative_url: str):
        return base_url + relative_url

    @staticmethod
    def format_bid_ask_value(value):
        if isinstance(value, str):
            if '@' in value:
                value = value.split('@')[1]
            value = value.replace(",", "")
            value = value.strip()
        return value

    async def _unsubscribe_channels(self, ws: WSAssistant):
        try:
            if ws is not None:
                payload = "utr"
                unsubscribe_trades_request: WSPlainTextRequest = WSPlainTextRequest(payload=payload)
                account_id = await self._connector.check_account_id_status()
                payload = f"ubd+{account_id}"
                unsubscribe_depth_request: WSPlainTextRequest = WSPlainTextRequest(payload=payload)
                await ws.send(unsubscribe_trades_request)
                await ws.send(unsubscribe_depth_request)
                self.logger().info("Unsubscribed to trade and depth data...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred unsubscribing to trades and orderbook data...",
                exc_info=True
            )
            raise

    async def keep_session_socket_alive(self, ws: WSAssistant):
        while True:
            payload = "tick"
            subscribe_trade_request: WSPlainTextRequest = WSPlainTextRequest(payload=payload)
            await ws.send(subscribe_trade_request)
            await self._sleep(60.0)  # needs to call this function after 60 seconds

    async def listen_for_subscriptions(self):
        """
        Connects to the trade events and order diffs websocket endpoints and listens to the messages sent by the
        exchange. Each message is stored in its own queue.
        """
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
            except Exception as e:
                self.logger().exception(
                    "Unexpected error occurred when listening to order book streams. Retrying in 5 seconds...",
                )
                await self._sleep(1.0)
            finally:
                await self._unsubscribe_channels(ws)
                await self._on_order_stream_interruption(websocket_assistant=ws)

    async def _connected_websocket_assistant(self) -> WSAssistant:
        try:
            payload = await self._connector.ping_session()
            ws: WSAssistant = await self._api_factory.get_ws_assistant()
            await ws.connect(ws_url=CONSTANTS.WSS_URL, ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
            subscribe_candles_request: WSJSONRequest = WSJSONRequest(payload=payload)
            await ws.send(subscribe_candles_request)
            return ws
        except asyncio.CancelledError:
            raise
        except ConnectionError as connection_exception:
            raise ConnectionError(f"The websocket connection was closed ({connection_exception})")
        except Exception as e:
            raise ValueError("Unexpected error occurred when listening to trades and depth. Retrying in 1 seconds...")

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Retrieves a copy of the full order book from the exchange, for a particular trading pair.

        Ibkr does not provide historical order book data. We can fetch latest data

        :param trading_pair: the trading pair for which the order book will be retrieved

        :return: the response from the exchange (JSON dictionary)
        """
        order_book = await self._connector.get_trading_pair_price(trading_pair=trading_pair)
        bid_keys = ["84", "88"]  # [bidPrice, bidSize]
        ask_keys = ["86", "85"]  # [askPrice, askSize]
        if order_book:
            bid_values = [self.format_bid_ask_value(order_book[key]) for key in bid_keys if key in order_book]
            ask_values = [self.format_bid_ask_value(order_book[key]) for key in ask_keys if key in order_book]
            order_book["bids"] = [bid_values]
            order_book["asks"] = [ask_values]
        return order_book

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.
        :param ws: the websocket assistant used to connect to the exchange
        """
        try:
            params = json.dumps({"days": 1})
            payload = f"str+{params}"
            subscribe_trade_request: WSPlainTextRequest = WSPlainTextRequest(payload=payload)
            await ws.send(subscribe_trade_request)
            for trading_pair in self._trading_pairs:
                symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                account_id = await self._connector.check_account_id_status()
                exchange = self._connector.ibkr_exchange_name
                payload = f"sbd+{account_id}+{symbol}+{exchange}"
                subscribe_depth_request: WSPlainTextRequest = WSPlainTextRequest(payload=payload)
                await ws.send(subscribe_depth_request)
                break
            self.logger().info("Subscribed to public order book and trade channels...")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().error(e, exc_info=True)
            raise

    def transform_data(self, original_data):
        transformed_data = {
            "asks": [],
            "bids": []
        }

        for entry in original_data.get("data", []):

            if "ask" in entry:
                transformed_data["asks"].append([self.format_bid_ask_value(entry["price"]),
                                                 self.format_bid_ask_value(entry["ask"])])
            if "bid" in entry:
                transformed_data["bids"].append([self.format_bid_ask_value(entry["price"]),
                                                 self.format_bid_ask_value(entry["bid"])])

        return transformed_data

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
        snapshot_timestamp: float = time.time()
        snapshot_msg: OrderBookMessage = IbkrOrderBook.snapshot_message_from_exchange(
            snapshot,
            snapshot_timestamp,
            metadata={"trading_pair": trading_pair}
        )
        return snapshot_msg

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        if "error" not in raw_message:
            for message in raw_message.get("args", []):
                trading_pair = [symbol for symbol in self._trading_pairs if message.get("symbol") in symbol]
                if trading_pair:
                    trading_pair = trading_pair[0]
                    message['size'] = self.format_bid_ask_value(str(message['size']))
                    message['price'] = self.format_bid_ask_value(str(message['price']))
                    trade_message = IbkrOrderBook.trade_message_from_exchange(
                        message, {"trading_pair": trading_pair})
                    message_queue.put_nowait(trade_message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        if "error" not in raw_message:
            con_id = raw_message["topic"].split("+")[2]
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=con_id)
            raw_message_transformed = self.transform_data(raw_message)
            if len(raw_message_transformed.get("asks")) > 0 or len(raw_message_transformed.get("bids")) > 0:
                order_book_message: OrderBookMessage = IbkrOrderBook.diff_message_from_exchange(
                    raw_message_transformed, time.time(), {"trading_pair": trading_pair})
                message_queue.put_nowait(order_book_message)

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        if "error" not in event_message:
            event_type = event_message.get("topic")
            channel = (self._diff_messages_queue_key if CONSTANTS.DIFF_EVENT_TYPE in event_type
                       else self._trade_messages_queue_key if CONSTANTS.TRADE_EVENT_TYPE in event_type else None)
            return channel

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant):
        async for ws_response in websocket_assistant.iter_messages():
            data: Dict[str, Any] = json.loads(ws_response.data.decode("UTF-8"))
            if data is not None:  # data will be None when the websocket is disconnected
                channel: str = self._channel_originating_message(event_message=data)
                valid_channels = self._get_messages_queue_keys()
                if channel in valid_channels:
                    self._message_queue[channel].put_nowait(data)
                else:
                    await self._process_message_for_unknown_channel(
                        event_message=data, websocket_assistant=websocket_assistant
                    )
