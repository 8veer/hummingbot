import os
import subprocess
from typing import Any, Dict, Callable, Awaitable
import time
import socket
from datetime import datetime
from decimal import Decimal

from pydantic import Field, SecretStr, validator

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap, ClientFieldData
from hummingbot.connector.exchange.ibkr import ibkr_constants as CONSTANTS
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "RELIANCE"

DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.0"),
    taker_percent_fee_decimal=Decimal("0.0")
)


def clean_traded_price(price: str):
    price = price.replace('C', '').replace('H', '')
    return float(price)


def convert_str_to_date(time_str: str, _format: str):
    return datetime.strptime(time_str, _format).date()


def get_ms_timestamp() -> int:
    return int(time.time() * 1e3)


def convert_str_time_to_timestamp(time_str: str, _format: str) -> int:
    return int(datetime.strptime(time_str, _format).timestamp())


async def is_authenticated_user(api_get: Callable[[str], Awaitable[Dict[str, Any]]], 
                                path_url: str = CONSTANTS.AUTH_CONFIRM_PATH_URL) -> bool:
    """
        Verified if Ibkr brokerage session is running and Ibkr client user is authenticated
    """
    try:
        auth_data = await api_get(path_url)
        return auth_data.get("authenticated", False) and auth_data.get("connected", False)
    except Exception as e:
        raise Exception(e)


def is_exchange_information_valid(exchange_info: Dict[str, Any]) -> bool:
    """
    Verifies if a trading pair is enabled to operate with based on its exchange information
    :param exchange_info: the exchange information for a trading pair
    :return: True if the trading pair is enabled, False otherwise
    """
    is_trading = False

    # 7768 is the key value which checks if user has trading permission or not 1 if user has trading permission else 0
    # 7184 is the key value which checks if contract can be traded or not 1 if contract is tradeable else 0
    if exchange_info.get("7768", "") == "1" and exchange_info.get("7184", "") == "1":
        is_trading = True

    return is_trading


def is_ibkr_session_running() -> bool:
    """
    Check if Ibkr session is already running by checking if the specific port is in use
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex((CONSTANTS.DEFAULT_DOMAIN, CONSTANTS.PORT)) == 0


def start_ibkr_session():
    try:
        if is_ibkr_session_running():
            return

        client_portal_session_path = os.path.join(CONSTANTS.CURRENT_WORKING_DIRECTORY,
                                                  CONSTANTS.CLIENT_PORTAL_SESSION_RELATIVE_PATH)

        if not os.path.exists(client_portal_session_path):
            raise FileNotFoundError(f"Client portal session path not found: {client_portal_session_path}")

        # Change Current Working Directory
        os.chdir(client_portal_session_path)
        command = ' '.join(CONSTANTS.BASH_COMMANDS)

        # Start the process
        result = subprocess.run(command, shell=True, capture_output=True, text=True, timeout=10)
        # time.sleep(5)

        # Restore current working directory
        os.chdir(CONSTANTS.CURRENT_WORKING_DIRECTORY)

        # Check the result
        if result.returncode != 0:
            raise RuntimeError(f"Command failed with return code {result.returncode}\n"
                               f"Command output:\n{result.stdout}\n"
                               f"Command error output:\n{result.stderr}")

    except subprocess.CalledProcessError as e:
        pass
    except Exception as e:
        pass


class IbkrConfigMap(BaseConnectorConfigMap):
    connector: str = Field(default="ibkr", client_data=None)
    ibkr_api_key: str = Field(
        default="Yes",
        client_data=ClientFieldData(
            prompt=lambda cm: f"Navigate to {CONSTANTS.BASE_URL} and sign in using your standard Interactive Brokers credentials."
                              f"\nAfter entering your information, you will see Client login succeeds to indicate a successful login. "
                              f"\nAfter successful login type Yes",
            is_connect_key=True,
            prompt_on_new=True,
        )
    )
    ibkr_secret_key: str = Field(
        default="NSE",
        client_data=ClientFieldData(
            prompt=lambda cm: f"Enter valid name of exchange you want to trade with (i.e. NSE).",
            is_connect_key=True,
            prompt_on_new=True,
        )
    )

    class Config:
        title = "ibkr"


KEYS = IbkrConfigMap.construct()
start_ibkr_session()
