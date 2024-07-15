import asyncio
from typing import TYPE_CHECKING, List, Optional, Any, Dict

from hummingbot.connector.exchange.ibkr.ibkr_auth import IbkrAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.ibkr.ibkr_exchange import IbkrExchange


class IbkrAPIUserStreamDataSource(UserStreamTrackerDataSource):

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 trading_pairs: List[str],
                 connector: 'IbkrExchange',
                 api_factory: WebAssistantsFactory):
        super().__init__()
        self._current_listen_key = None
        self._api_factory = api_factory

        self._listen_key_initialized_event: asyncio.Event = asyncio.Event()
        self._last_listen_key_ping_ts = 0

    async def _connected_websocket_assistant(self) -> WSAssistant:
        pass

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        pass

    async def _get_listen_key(self):
        pass

    async def _ping_listen_key(self) -> bool:
        pass

    async def _manage_listen_key_task_loop(self):
        pass

    @classmethod
    def logger(cls) -> HummingbotLogger:
        pass

    @property
    def last_recv_time(self) -> float:
        return 0

    async def _get_ws_assistant(self) -> WSAssistant:
        pass

    async def _on_user_stream_interruption(self, websocket_assistant: Optional[WSAssistant]):
        pass

    async def listen_for_user_stream(self, output: asyncio.Queue):
        pass

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant, queue: asyncio.Queue):
        pass

    async def _process_event_message(self, event_message: Dict[str, Any], queue: asyncio.Queue):
        pass

    async def _send_ping(self, websocket_assistant: WSAssistant):
        pass

    async def _sleep(self, delay: float):
        pass

    def _time(self) -> float:
        pass
