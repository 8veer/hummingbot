from bidict import bidict
from datetime import timedelta

from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit

# Base URL
BASE_URL = "https://localhost:5000"
DEFAULT_DOMAIN = "localhost"
DEFAULT_PORT = 5000
PORT = 5000
REST_URL = f"{BASE_URL}/v1/api"
WSS_URL = f"wss://{DEFAULT_DOMAIN}:{DEFAULT_PORT}/v1/api/ws"

HEALTH_CHECK_ENDPOINT = "/tickle"
CANDLES_ENDPOINT = "/iserver/marketdata/history"
EXCHANGE_URL = "/trsrv/all-conids"


AUTH_EXCEPTION = "Please make sure your ibkr session is running and authenticated"

INTERVALS = bidict({
    "1m": "1min",
    "2m": "2min",
    "3m": "3min",
    "5m": "5min",
    "10m": "10min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1h",
    "2h": "2h",
    "3h": "3h",
    "4h": "4h",
    "8h": "8h",
    "1d": "1d",
    "1w": "1w",
    "1M": "1m"
})

INTERVALS_TO_PERIOD = bidict({
    "1min": "3d",
    "2min": "6d",
    "3min": "9d",
    "5min": "14d",
    "10min": "28d",
    "15min": "42d",
    "30min": "84d",
    "1h": "34w",
    "2h": "67w",
    "3h": "100w",
    "4h": "135w",
    "8h": "268w",
    "1d": "50m",
    "1w": "14y",
    "1m": "15y"
})

INTERVAL_DEFAULT_PERIOD = {
    "1min": "1d",
    "2min": "1d",
    "3min": "1d",
    "5min": "1d",
    "10min": "1d",
    "15min": "1d",
    "30min": "1d",
    "1h": "1w",
    "2h": "1w",
    "3h": "1w",
    "4h": "1w",
    "8h": "1w",
    "1d": "1m",
    "1w": "1y",
    "1m": "1y"
}

INTERVAL_TO_SECONDS = {
    '1min': 60,
    '2min': 120,
    '3min': 180,
    '5min': 300,
    '10min': 600,
    '15min': 900,
    '30min': 1800,
    '1h': 3600,
    '2h': 7200,
    '3h': 10800,
    '4h': 14400,
    '8h': 28800,
    '1d': 86400,
    '1w': 604800,
    '1m': 2592000
}

INTERVAL_MAPPING = {
        '1min': timedelta(minutes=1),
        '2min': timedelta(minutes=2),
        '3min': timedelta(minutes=3),
        '5min': timedelta(minutes=5),
        '10min': timedelta(minutes=10),
        '15min': timedelta(minutes=15),
        '30min': timedelta(minutes=30),
        '1h': timedelta(hours=1),
        '2h': timedelta(hours=2),
        '3h': timedelta(hours=3),
        '4h': timedelta(hours=4),
        '8h': timedelta(hours=8),
        '1d': timedelta(days=1),
        '1w': timedelta(weeks=1),
        '1m': timedelta(days=30)
    }


RATE_LIMITS = [
    RateLimit(CANDLES_ENDPOINT, limit=5, time_interval=1),
    RateLimit(HEALTH_CHECK_ENDPOINT, limit=1, time_interval=1),
]
