from datetime import datetime, timezone, timedelta
from hummingbot.client.settings import SCRIPT_STRATEGIES_PCR_PATH, SCRIPT_STRATEGY_CONF_DIR_PATH

SIGMA_FILE_NAME = SCRIPT_STRATEGIES_PCR_PATH / "iv.csv"
SIZES_FILE_NAME = SCRIPT_STRATEGIES_PCR_PATH / "sizes.csv"
CONFIG_FILE_NAME = SCRIPT_STRATEGIES_PCR_PATH / "config.txt"
SCRIPT_CONFIG_FILE_NAME = SCRIPT_STRATEGY_CONF_DIR_PATH / "pcr_threshold.yml"

INDIAN_STANDARD_STARTING_TIME = datetime.now(timezone(timedelta(hours=5.5))).replace(hour=9, minute=20)
INDIAN_STANDARD_END_TIME = datetime.now(timezone(timedelta(hours=5.5))).replace(hour=15, minute=25)

stop_time_fetched_data = datetime.now(timezone(timedelta(hours=5.5))).replace(hour=15, minute=25)
start_time_fetched_data = datetime.now(timezone(timedelta(hours=5.5))).replace(hour=9, minute=20)
end_time_fetched_data = datetime.now(timezone(timedelta(hours=5.5))).replace(hour=14, minute=50)
time_1_fetched_data = datetime.now(timezone(timedelta(hours=5.5))).replace(hour=9, minute=45)

stop_time_monitor_data = datetime.now(timezone(timedelta(hours=5.5))).replace(hour=15, minute=20)
end_time_monitor_data = datetime.now(timezone(timedelta(hours=5.5))).replace(hour=14, minute=50)
start_time_monitor_data = datetime.now(timezone(timedelta(hours=5.5))).replace(hour=9, minute=35)

start_time_exit_condition = datetime.now(timezone(timedelta(hours=5.5))).replace(hour=9, minute=18)

batch_size = 10
