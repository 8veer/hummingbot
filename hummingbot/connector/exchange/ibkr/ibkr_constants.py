import os
from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState
from datetime import datetime, timedelta, timezone

# Bash Command To Run Ibkr Brokerage Session
CURRENT_WORKING_DIRECTORY = os.getcwd()
CLIENT_PORTAL_SESSION_RELATIVE_PATH = "resources/clientportal.gw"
# BASH_COMMANDS = [r"bin\run.bat", r"root\conf.yaml"]
BASH_COMMANDS = ["bin/run.sh", "root/conf.yaml"]


HBOT_ORDER_ID_PREFIX = "HBOT"
MAX_ORDER_ID_LEN = 50
EXCHANGE_NAME = "ibkr"

DEFAULT_EXCHANGE = "NSE"
DEFAULT_CURRENCY = "INR"
WS_HEARTBEAT_TIME_INTERVAL = 30
CHUNK_SIZE = 200
HISTORICAL_CHUNK_SIZE = 5
START_TIME = datetime.now(timezone(timedelta(hours=5.5))).replace(hour=9, minute=30)
END_TIME = datetime.now(timezone(timedelta(hours=5.5))).replace(hour=15, minute=20)


# Base URL
DEFAULT_DOMAIN = "localhost"
PORT = 5000
BASE_URL = f"https://{DEFAULT_DOMAIN}:{PORT}"
REST_URL = "{}/v1/api"
WSS_URL = f"wss://{DEFAULT_DOMAIN}:{PORT}/v1/api/ws"

# Auth URL
AUTH_CONFIRM_PATH_URL = "/iserver/auth/status"
PING_PATH_URL = "/tickle"

PUBLIC_API_VERSION = "v1"

# Ibkr Client Path
ACCOUNT_PATH_URL = "/iserver/accounts"
ORDER_PATH_URL = "/iserver/account/{}/orders"
ORDER_CANCEL_URL = "/iserver/account/{}/order"
GET_ALL_CONRACTS_EXCHANGE = "/trsrv/all-conids"
MY_TRADES_PATH_URL = "/iserver/account/trades"
EXCHANGE_INFO_PATH_URL = "/iserver/contract/rules"
CONTRACT_MARKET_DATA = "/iserver/marketdata/snapshot"
CONTRACT_RULES = "/trsrv/secdef"
REPLY_ORDER = "/iserver/reply"
CURRENCY_PATH = "/portfolio/accounts"
BALANCE_PATH_URL = "/portfolio/{}/ledger"
ORDER_STATUS_PATH_URL = "/iserver/account/order/status/{}"
FUTURE_CONTRACT_PATH_URL = "/trsrv/futures"
MARKET_HISTORY_PATH_URL = "/iserver/marketdata/history"

# Options Chain Path
NSE_MAIN = "/"
NSE_DOMAIN = "https://www.nseindia.com/option-chain"
NSE_OPTION_CHAIN = "https://www.nseindia.com/api/option-chain-equities"
NSE_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    'Accept': '*/*',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'Sec-Ch-Ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': '"Windows"',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'cross-site',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1'
}


# Order params
SIDE_BUY = "BUY"
SIDE_SELL = "SELL"
CANCELLED_ORDER_MESSAGE = "Request was submitted"
GOOD_TILL_CANCEL = "GTC"
TRADING_DAY = "DAY"

# Event types
TRADE_EVENT_TYPE = "str"
DIFF_EVENT_TYPE = "sbd"

# Rate Limit time intervals
ONE_MINUTE = 60
ONE_SECOND = 1

# Order States
ORDER_STATE = {
    "Inactive": OrderState.PENDING_CREATE,
    "PendingSubmit": OrderState.PENDING_CANCEL,
    "PreSubmitted": OrderState.PENDING_CREATE,
    "Submitted": OrderState.APPROVED,
    "Filled": OrderState.FILLED,
    "PendingCancel": OrderState.PENDING_CANCEL,
    "Cancelled": OrderState.CANCELED
}

""" 
In order to get market related data from Ibkr we have to provide comma seprated fields as params
        e.g. fields=31,55, 82, 83, 84, 85, 86, 88
        31 => Last Price
        55 => Symbol
        82 => Change
        83 => Change %
        84 => Bid Price
        85 => Ask Size (For US stocks, the number displayed is divided by 100)
        86 => Ask Price
        88 => Bid Size (For US stocks, the number displayed is divided by 100)
        7094 => Conid + Exchange
        7184 => canBeTraded
        7768 => hasTradingPermissions
 """

CONVERT_KEY_TO_VALUE = {
    "31": "lastPrice",
    "55": "symbol",
    "84": "bidPrice",
    "85": "askSize",
    "86": "askPrice",
    "88": "bidSize",
    "7094": "conidEx"
}


MARKET_DATA_FIELDS = ",".join(["31", "55", "82", "83", "84", "85", "86", "88", "7094", "7184", "7768"])
LAST_TRADED_FIELD = ",".join(["31", "55"])
TRADING_PERMISSION_FIELDS = ",".join(["55", "7184", "7768"])

MIN_REQUEST_LIMIT = 1
MAX_REQUEST_LIMIT = 10

# Exceptions
AUTH_EXCEPTION = "Please make sure your ibkr session is running and authenticated"

RATE_LIMITS = [

    # Weighted Limits
    RateLimit(limit_id=ACCOUNT_PATH_URL, limit=MIN_REQUEST_LIMIT, time_interval=ONE_SECOND),
    RateLimit(limit_id=ORDER_PATH_URL, limit=MIN_REQUEST_LIMIT, time_interval=5 * ONE_SECOND),
    RateLimit(limit_id=ORDER_CANCEL_URL, limit=MIN_REQUEST_LIMIT, time_interval=5 * ONE_SECOND),
    RateLimit(limit_id=GET_ALL_CONRACTS_EXCHANGE, limit=MIN_REQUEST_LIMIT, time_interval=ONE_SECOND),
    RateLimit(limit_id=MY_TRADES_PATH_URL, limit=MAX_REQUEST_LIMIT, time_interval=ONE_SECOND),
    RateLimit(limit_id=EXCHANGE_INFO_PATH_URL, limit=MIN_REQUEST_LIMIT, time_interval=5 * ONE_SECOND),
    RateLimit(limit_id=CONTRACT_MARKET_DATA, limit=MAX_REQUEST_LIMIT, time_interval=ONE_SECOND),
    RateLimit(limit_id=AUTH_CONFIRM_PATH_URL, limit=MAX_REQUEST_LIMIT, time_interval=ONE_SECOND),
    RateLimit(limit_id=PING_PATH_URL, limit=MIN_REQUEST_LIMIT, time_interval=ONE_SECOND),
    RateLimit(limit_id=CURRENCY_PATH, limit=MIN_REQUEST_LIMIT, time_interval=5 * ONE_SECOND),
    RateLimit(limit_id=CONTRACT_RULES, limit=MIN_REQUEST_LIMIT, time_interval=ONE_SECOND),
    RateLimit(limit_id=ORDER_STATUS_PATH_URL, limit=MAX_REQUEST_LIMIT, time_interval=ONE_SECOND),
    RateLimit(limit_id=REPLY_ORDER, limit=MIN_REQUEST_LIMIT, time_interval=ONE_SECOND),
    RateLimit(limit_id=FUTURE_CONTRACT_PATH_URL, limit=MAX_REQUEST_LIMIT, time_interval=ONE_SECOND),
    RateLimit(limit_id=BALANCE_PATH_URL, limit=MAX_REQUEST_LIMIT, time_interval=ONE_SECOND),
    RateLimit(limit_id=NSE_MAIN, limit=MAX_REQUEST_LIMIT, time_interval=ONE_SECOND),
    RateLimit(limit_id=NSE_OPTION_CHAIN, limit=MAX_REQUEST_LIMIT, time_interval=ONE_SECOND),
    RateLimit(limit_id=MARKET_HISTORY_PATH_URL, limit=5, time_interval=ONE_SECOND),
]
