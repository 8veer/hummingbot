import aiohttp
import asyncio
import ssl
import json
import pytest
from decimal import Decimal
from typing import Awaitable

from aioresponses import aioresponses

from hummingbot.connector.exchange.ibkr import ibkr_constants as CONSTANTS, ibkr_web_utils as web_utils
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.rate_oracle.sources.ibkr_rate_source import IbkrRateSource


@pytest.fixture(scope="module")
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="module")
def setup():
    target_token = "RELIANCE"
    global_token = "INR"
    trading_pair = combine_to_hb_trading_pair(base=target_token, quote=global_token)
    ignored_trading_pair = combine_to_hb_trading_pair(base="SOME", quote="PAIR")
    return target_token, global_token, trading_pair, ignored_trading_pair


async def auth_status():
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context)) as session:
        try:
            async with session.get("https://localhost:5000/v1/api/iserver/auth/status") as response:
                response.raise_for_status()
                return await response.json()
        except aiohttp.ClientError as e:
            print(f"Request failed: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")


async def async_run_with_timeout(coroutine: Awaitable, timeout: int = 360):
    return await asyncio.wait_for(coroutine, timeout)


@pytest.fixture
def aiohttp_mock():
    with aioresponses() as m:
        yield m


@pytest.mark.asyncio
async def test_get_ibkr_prices(setup, aiohttp_mock):
    target_token, global_token, trading_pair, ignored_trading_pair = setup
    # rate_source = IbkrRateSource()

    # Mock the response
    # aiohttp_mock.get("https://localhost:5000/v1/api/iserver/auth/status", payload={"status": "success"})

    prices = await async_run_with_timeout(auth_status())
    print(prices)

    # Uncomment and modify the following lines according to your actual test
    # expected_rate = Decimal("10")
    # assert trading_pair in prices
    # assert prices[trading_pair] == expected_rate
    # assert ignored_trading_pair not in prices
