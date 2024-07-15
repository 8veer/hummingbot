import math
import json

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import TradeType
from scripts.pcr import (constants as CONSTANTS, utils, calculations)
from datetime import datetime, timedelta, timezone
from hummingbot.connector.exchange.ibkr.ibkr_exchange import IbkrExchange
from hummingbot.strategy_v2.executors.pcr_executor.data_types import PCRExecutorConfig
from hummingbot.strategy.strategy_v2_base import *


class PcrThresholdConfig(StrategyV2ConfigBase):
    markets: Dict[str, Set[str]] = {}
    script_file_name: str = ""
    exchange: str = "ibkr"
    candles_config: List[str] = []
    trading_pair: str = "RELIANCE"
    controllers_config: Optional[List[str]] = []
    long_positions: Optional[int] = 0
    short_positions: Optional[int] = 0
    total_positions: Optional[int] = 0
    max_long_positions: Optional[int] = 6
    max_short_positions: Optional[int] = 6
    max_total_positions: Optional[int] = 10
    stock_list: Optional[List[str]] = []
    ban_list: Optional[List[str]] = []
    level_1_targets: Optional[List[float]] = []
    level_2_targets: Optional[List[float]] = []
    stop_losses: Optional[List[float]] = []
    pcr_at_threshold_long: Optional[float] = None
    pcr_at_threshold_short: Optional[float] = None
    entry_levels_long_lower_limit: Optional[float] = -0.25
    entry_levels_long_upper_limit: Optional[float] = 0.1
    entry_levels_short_lower_limit: Optional[float] = -0.25
    entry_levels_short_upper_limit: Optional[float] = 0.1
    trailing_sl_trigger_level_long: Optional[float] = 0.5
    trailing_sl_trigger_level_short: Optional[float] = 0.5
    expiry_date: Optional[str]
    contract_expiry_date: Optional[str]
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    time_limit: Optional[float] = None


class PcrThreshold(StrategyV2Base):

    @classmethod
    def init_markets(cls, config: PcrThresholdConfig):
        cls.stocks_to_monitor = list(filter(lambda x: x not in config.ban_list, config.stock_list))
        cls.markets = {config.exchange: set(cls.stocks_to_monitor)}

    def update_connector_base(self, connectors: Dict[str, ConnectorBase], connector_name: str):
        stock_list = [item[:9].replace('&', '') for item in self.stocks_to_monitor]
        connectors[connector_name].trading_pairs.clear()
        connectors[connector_name].trading_pairs.extend(stock_list)

    def __init__(self, connectors: Dict[str, ConnectorBase], config: PcrThresholdConfig):
        self.stocks_to_monitor = list(filter(lambda x: x not in config.ban_list, config.stock_list))
        self.update_connector_base(connectors, config.exchange)
        super().__init__(connectors, config)
        self.config = config
        self.prices: dict = {}
        self.option_chains: dict = {}
        self.future_contract: dict = {}
        self.future_prices: dict = {}
        self.positions: dict = {}
        self.today_positions: dict = {}
        self.levels: dict = {}
        self.symbols_with_signals: dict = {}
        self.highs: dict = {}
        self.lows: dict = {}
        self.fair_price: dict = {}
        self.underlying = utils.create_symbol_dict(self.stocks_to_monitor)
        self.sigma = utils.read_csv_to_dict(CONSTANTS.SIGMA_FILE_NAME)
        self.sizes = utils.read_sizes_to_dict(CONSTANTS.SIZES_FILE_NAME)
        self.long_positions: int = self.config.long_positions
        self.short_positions: int = self.config.short_positions
        self.total_positions: int = self.config.total_positions
        self.max_long_positions: int = self.config.max_long_positions
        self.max_short_positions: int = self.config.max_short_positions
        self.max_total_positions: int = self.config.max_total_positions
        self.pcr_at_threshold_long: Optional[float] = None
        self.pcr_at_threshold_short: Optional[float] = None
        self.entry_levels_long_lower_limit: Optional[float] = self.config.entry_levels_long_lower_limit
        self.entry_levels_long_upper_limit: Optional[float] = self.config.entry_levels_long_upper_limit
        self.entry_levels_short_lower_limit: Optional[float] = self.config.entry_levels_short_lower_limit
        self.entry_levels_short_upper_limit: Optional[float] = self.config.entry_levels_short_upper_limit
        self.trailing_sl_trigger_level_long: Optional[float] = self.config.trailing_sl_trigger_level_long
        self.trailing_sl_trigger_level_short: Optional[float] = self.config.trailing_sl_trigger_level_short
        self.ticker: dict = {}
        self.threshold: dict = {}
        self.stop_loss_price: dict = {}
        self.target_price: dict = {}
        self.entry_prices: dict = {}
        self.short_target_factor: dict = {}
        self.trailing_factor: dict = {}
        self.expiry_date: str = self.config.expiry_date
        self.contract_expiry_date: str = self.config.contract_expiry_date
        self.batch_index: int = -1

    @property
    def start_condition(self):
        now = datetime.now(timezone(timedelta(hours=5.5)))
        return CONSTANTS.stop_time_fetched_data >= now >= CONSTANTS.start_time_exit_condition

    async def on_tick(self):
        if self.start_condition:
            self.update_executors_info()
            self.update_controllers_configs()
            if self.market_data_provider.ready:
                if await self.market_data_provider.session_check(self.config.exchange):
                    await self.update_processed_data()
                    executor_actions: List[ExecutorAction] = self.determine_executor_actions()
                    for action in executor_actions:
                        self.executor_orchestrator.execute_action(action)
                else:
                    self.logger().error("Please check your network session and session aut")
        else:
            self.logger().error("The script is active only within trading hours. Enter Stop command to stop the script")

    async def update_processed_data(self):
        now = datetime.now(timezone(timedelta(hours=5.5)))

        if now >= CONSTANTS.stop_time_fetched_data:
            return

        self.prices = await self.market_data_provider.get_all_last_traded_price(self.config.exchange)
        self.future_contract = await self.market_data_provider.get_all_future_contracts(self.config.exchange,
                                                                                        self.contract_expiry_date)
        all_future_con_ids = [str(item) for item in self.future_contract.values()]
        self.future_prices = await self.market_data_provider.get_all_last_traded_price(self.config.exchange,
                                                                                       all_future_con_ids)
        await self.continuously_fetched_data()

    async def continuously_fetched_data(self):
        now = datetime.now(timezone(timedelta(hours=5.5)))

        if now >= CONSTANTS.stop_time_fetched_data:
            return

        if self.batch_index < math.floor(len(self.stocks_to_monitor) / CONSTANTS.batch_size):
            self.batch_index += 1
        else:
            self.batch_index = 0

        stock_batch = self.stocks_to_monitor[
                      self.batch_index * CONSTANTS.batch_size: (self.batch_index + 1) * CONSTANTS.batch_size]

        self.option_chains = await self.market_data_provider.get_all_option_chain(self.config.exchange,
                                                                                  self.stocks_to_monitor)

        for symbol in stock_batch:
            data = self.option_chains.get(symbol)
            if data is None:
                continue
            spot_price = calculations.calculate_points(data, self.expiry_date)
            underlying = symbol[:9].replace('&', '')

            if CONSTANTS.start_time_fetched_data <= now < CONSTANTS.end_time_fetched_data:

                try:
                    stock_sigma = float(self.sigma.get(symbol))
                    self.threshold[symbol] = calculations.calculate_thresholds(spot_price)

                    pcr_at_threshold = calculations.calculate_pcr(data, self.threshold.get(symbol), self.expiry_date)
                    near_threshold: bool = (
                                abs((spot_price - self.threshold.get(symbol)) * 100 / spot_price) <= 1 * stock_sigma)

                    if underlying not in self.positions.copy():

                        if pcr_at_threshold < 1 and near_threshold:

                            self.symbols_with_signals[symbol] = "Short"

                        elif pcr_at_threshold > 1 and near_threshold:

                            self.symbols_with_signals[symbol] = "Long"

                        else:
                            if symbol in self.symbols_with_signals.copy():
                                del self.symbols_with_signals[symbol]
                except Exception as e:
                    self.logger().error(f"Error occur in continuously fetched data for symbol {symbol}")
                    self.logger().error(e)

            elif now >= CONSTANTS.end_time_fetched_data:

                if symbol in self.symbols_with_signals.copy():
                    del self.symbols_with_signals[symbol]

    def determine_executor_actions(self) -> List[ExecutorAction]:
        """
        Determine actions based on the provided executor handler report.
        """
        actions = []
        now = datetime.now(timezone(timedelta(hours=5.5)))
        if CONSTANTS.stop_time_monitor_data > now >= CONSTANTS.start_time_monitor_data and len(
                self.symbols_with_signals.copy()) > 0:
            actions.extend(self.create_actions_proposal())
        return actions

    def can_create_executor(self, pos):
        if pos == "Long" and self.long_positions < self.max_long_positions:
            return True
        elif pos == "Short" and self.short_positions < self.max_short_positions:
            return True
        return False

    def get_active_executors(self, connector_name: str):
        active_executors = self.filter_executors(
            executors=self.get_all_executors(),
            filter_func=lambda e: e.connector_name == connector_name and e.is_active
        )
        return active_executors

    def stop_actions_proposal(self) -> List[StopExecutorAction]:
        stop_actions = []
        active_executors = self.get_active_executors(self.config.exchange)
        if active_executors:
            stop_actions.extend([StopExecutorAction(executor_id=e.id) for e in active_executors])
        return stop_actions

    def create_actions_proposal(self) -> List[CreateExecutorAction]:
        self.logger().info("Create Executor Action Called")
        create_actions = []
        now = datetime.now(timezone(timedelta(hours=5.5)))
        if now <= CONSTANTS.end_time_monitor_data:
            for symbol in self.symbols_with_signals.copy():

                underlying = symbol[:9].replace('&', '')

                market_price = self.prices.get(underlying)
                if market_price is None:
                    self.logger().error(
                        f"No Market Data Found for {underlying}")
                    continue
                stock_sigma = float(self.sigma.get(symbol))
                con_id = self.future_contract.get(underlying)

                if con_id is None:
                    self.logger().error(
                        f"No Future Contract Found for {underlying} for expiry date {self.contract_expiry_date}")
                    continue
                pos = self.symbols_with_signals.get(symbol)
                level_id = self.levels.get(underlying)

                if self.total_positions < self.max_total_positions and not self.today_positions.get(underlying):

                    if self.can_create_executor(pos):
                        create_actions.append(CreateExecutorAction(
                            executor_config=PCRExecutorConfig(
                                timestamp=self.current_timestamp,
                                connector_name=self.config.exchange,
                                trading_pair=underlying,
                                symbol=symbol,
                                underlying=underlying,
                                side=TradeType.BUY if pos == "Long" else TradeType.SELL,
                                strategy=self,
                                pos=pos,
                                process_exit=False,
                                market_price=market_price,
                                stock_sigma=stock_sigma,
                                level_id=level_id,
                                con_id=con_id,
                                future="future"
                            )))
        create_actions = self.check_exit_conditions(create_actions)
        return create_actions

    def check_exit_conditions(self, create_actions) -> List[CreateExecutorAction]:
        now = datetime.now(timezone(timedelta(hours=5.5)))
        if now >= CONSTANTS.start_time_exit_condition:
            config_file = utils.extract_stocks_from_file(CONSTANTS.CONFIG_FILE_NAME)
            for symbol in self.positions.copy():

                underlying = self.underlying.get(symbol)
                market_price = self.future_prices.get(symbol)
                stock_sigma = float(self.sigma.get(underlying))
                pos = self.positions.get(symbol)
                con_id = self.future_contract.get(symbol)

                if con_id is None:
                    self.logger().error(
                        f"No Future Contract Found for {symbol} for expiry date {self.contract_expiry_date}")
                    continue
                create_actions.append(CreateExecutorAction(
                    executor_config=PCRExecutorConfig(
                        connector_name=self.config.exchange,
                        timestamp=self.current_timestamp,
                        trading_pair=symbol,
                        symbol=symbol,
                        underlying=underlying,
                        side=TradeType.BUY,
                        strategy=self,
                        pos=pos,
                        process_exit=True,
                        exit_config=config_file,
                        market_price=market_price,
                        stock_sigma=stock_sigma,
                        level_id=self.levels.get(symbol),
                        con_id=con_id,
                        future="future"
                    )))
        return create_actions
