from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class IbkrAuth(AuthBase):

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """
        This method is intended to configure a rest request to be authenticated. Ibkr does not use this
        functionality
        """
        return request  # pass-through

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        """
        This method is intended to configure a websocket request to be authenticated. Ibkr does not use this
        functionality
        """
        return request  # pass-through
