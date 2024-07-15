import logging
from decimal import Decimal
from typing import List, Optional, Dict, Union
from datetime import datetime, timedelta, timezone


from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType, PositionAction, PriceType, TradeType
from hummingbot.core.event.events import (
    BuyOrderCreatedEvent,
    MarketOrderFailureEvent,
    OrderFilledEvent,
    SellOrderCreatedEvent,
)
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.strategy_v2.executors.executor_base import ExecutorBase
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder
from hummingbot.strategy_v2.executors.pcr_executor.data_types import PCRExecutorConfig


class PCRExecutor(ExecutorBase):
    _logger = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, strategy: ScriptStrategyBase, config: PCRExecutorConfig, update_interval: float = 1.0):

        # Initialize super class
        super().__init__(strategy=strategy, connectors=[config.connector_name], config=config,
                         update_interval=update_interval)
        self.config: PCRExecutorConfig = config

        # executors tracking
        self._open_orders: List[TrackedOrder] = []
        self._close_orders: List[TrackedOrder] = []  # for now will be just one order but we can have multiple
        self._failed_orders: List[TrackedOrder] = []

    def get_net_pnl_quote(self) -> Decimal:
        """
        Returns the net profit or loss in quote currency.
        """
        return Decimal(0)

    def get_net_pnl_pct(self) -> Decimal:
        """
        Returns the net profit or loss in percentage.
        """
        return Decimal(0)

    def get_cum_fees_quote(self) -> Decimal:
        """
        Returns the cumulative fees in quote currency.
        """
        return Decimal(0)

    @property
    def current_time(self):
        return datetime.now(timezone(timedelta(hours=5.5)))

    @property
    def active_open_orders(self) -> List[TrackedOrder]:
        return self._open_orders

    @property
    def active_close_orders(self) -> List[TrackedOrder]:
        return self._close_orders

    @property
    def close_order_type(self) -> OrderType:
        return OrderType.MARKET

    @property
    def open_order_type(self) -> OrderType:
        return OrderType.LIMIT

    @property
    def current_market_price(self):
        return self.config.market_price

    @property
    def current_stock_sigma(self):
        return self.config.stock_sigma

    @property
    def entry_level(self):
        if self.config.pos == "Long":
            entry_level = (
                    (self.current_market_price - self.config.strategy.threshold.get(self.config.symbol)) * 100 / (
                     self.current_stock_sigma * self.current_market_price))
        else:
            entry_level = (
                        (self.config.strategy.threshold.get(self.config.symbol) - self.current_market_price) * 100 / (
                         self.current_stock_sigma * self.current_market_price))
        return entry_level

    def close_execution_by(self, close_type):
        self.close_type = close_type
        self.close_timestamp = self._strategy.current_timestamp
        self.stop()

    def early_stop(self):
        # self.cancel_open_orders()
        self.close_execution_by(CloseType.EARLY_STOP)

    def trading_time(self):
        return self.current_time < self.config.stop_time

    def on_start(self):
        super().on_start()
        if not self.trading_time():
            self.close_execution_by(CloseType.EARLY_STOP)

    async def control_task(self):
        """
        This method is responsible for controlling the task based on the status of the executor.

        :return: None
        """
        if self.status == RunnableStatus.RUNNING and not self.config.order_placed:
            self.logger().info(f"Executor Started For Symbol: {self.config.symbol}")
            self.config.order_placed = True
            self.process_order_condition()
        # elif self.status == RunnableStatus.SHUTTING_DOWN:
        #     self.close_execution_by(CloseType.EXPIRED)

    def can_process_order(self, pos):
        if self.config.strategy.total_positions < self.config.strategy.max_total_positions:
            if pos == "Long" and self.config.strategy.long_positions < self.config.strategy.max_long_positions:
                return True
            elif pos == "Short" and self.config.strategy.short_positions < self.config.strategy.max_short_positions:
                return True
        return False

    def process_order_condition(self):
        if not self.config.process_exit and self.can_process_order(self.config.pos) and -0.25 <= self.entry_level <= 0.1:
            self.config.strategy.positions[self.config.underlying] = self.config.pos
            self.config.strategy.today_positions[self.config.underlying] = True
            self.config.strategy.levels[self.config.underlying] = 1
            self.position_update()
            self.create_order(self.config.underlying, self.config.side,
                              self.config.strategy.levels.get(self.config.underlying))
        elif self.config.process_exit:
            self.process_exit_order()
        else:
            self.close_execution_by(CloseType.EARLY_STOP)

    def create_order(self, symbol, side, level, close_order=False, price=None):
        try:
            kwargs = dict(con_id=self.config.con_id, argument=self.config.future)
            underlying = self.config.strategy.underlying.get(symbol)
            multiplier = level if level != 0 else self.config.strategy.levels.get(symbol)
            order_type = self.open_order_type if price else self.close_order_type
            quantity = self.config.strategy.sizes.get(underlying, 0.0) * multiplier
            if quantity <= 0:
                self.logger().error("Order Amount is less than 1")
                self.close_execution_by(CloseType.FAILED)
            price = 1.0 if price is None else price
            order_id = self.place_order(connector_name=self.config.connector_name,
                                        trading_pair=symbol, order_type=order_type,
                                        side=side, amount=Decimal(quantity), price=Decimal(price),
                                        position_action=PositionAction.OPEN,
                                        **kwargs)
            if order_id and not close_order:
                self._open_orders.append(TrackedOrder(order_id=order_id))
            elif order_id:
                self._close_orders.append(TrackedOrder(order_id=order_id))
        except Exception as e:
            self.logger().error(f"Exception occur while placing order for symbol {symbol} because of {e}")
            self.close_execution_by(CloseType.FAILED)

    def position_update(self, decrement=False):
        if not decrement:
            if self.config.pos == "Long":
                self.config.strategy.long_positions += 1
            else:
                self.config.strategy.short_positions += 1
            self.config.strategy.total_positions += 1
        else:
            if self.config.pos == "Long":
                self.config.strategy.long_positions -= 1
            else:
                self.config.strategy.short_positions -= 1
            self.config.strategy.total_positions -= 1

    def process_exit_order(self):
        now = datetime.now(timezone(timedelta(hours=5.5)))
        end_time = datetime.now(timezone(timedelta(hours=5.5))).replace(hour=15, minute=10)
        symbol = self.config.symbol
        market_price = self.current_market_price
        stock_sigma = self.current_stock_sigma
        pos = self.config.pos
        try:
            if pos == "Long":

                threshold = self.config.strategy.entry_prices.get(symbol)
                n = ((market_price - threshold) * 100 / (stock_sigma * threshold))

                if market_price >= self.config.strategy.target_price.get(
                        symbol) or ("Exit" in self.config.exit_config) or (now >= end_time) or (
                        symbol in self.config.exit_config) or market_price <= self.config.strategy.stop_loss_price.get(
                        symbol):

                    del self.config.strategy.positions[symbol]
                    level = 0
                    self.create_order(symbol, TradeType.SELL, level, close_order=True)
                    self.position_update(decrement=True)

                elif n < -0.9 and self.config.strategy.levels.get(symbol) == 1:
                    self.config.strategy.levels[symbol] = 2
                    self.create_order(symbol, TradeType.BUY, self.config.strategy.levels.get(symbol))

                elif (-0.9 < n < -0.65) and self.config.strategy.levels.get(symbol) == 1:
                    self.config.strategy.target_price[symbol] = self.config.strategy.entry_prices.get(symbol) * (
                            1 + (0.3 * stock_sigma / 100))
                    self.logger().info(
                        f'Target Price is updated for {symbol} to {self.config.strategy.target_price.get(symbol)}')
                    self.close_execution_by(CloseType.TAKE_PROFIT)

                elif (-0.65 < n < -0.4) and self.config.strategy.levels.get(symbol) == 3:
                    self.config.strategy.target_price[symbol] = self.config.strategy.entry_prices.get(symbol) * (
                            1 + (0 * stock_sigma / 100))
                    self.logger().info(
                        f'Target Price is updated for {symbol} to {self.config.strategy.target_price.get(symbol)}')
                    self.close_execution_by(CloseType.TAKE_PROFIT)

                elif n > 0.5:
                    self.config.strategy.stop_loss_price[symbol] = self.config.strategy.entry_prices.get(symbol) * (
                            1 + (0.3 * stock_sigma / 100))
                    self.logger().info(
                        f'Stop Loss Price is updated for {symbol} to {self.config.strategy.stop_loss_price.get(symbol)}')
                    self.close_execution_by(CloseType.STOP_LOSS)
                else:
                    self.close_execution_by(CloseType.EXPIRED)

            elif pos == "Short":

                threshold = self.config.strategy.entry_prices.get(symbol)
                n = ((threshold - market_price) * 100 / (stock_sigma * threshold))

                if (market_price <= self.config.strategy.target_price.get(symbol) or (
                        "Exit" in self.config.exit_config) or (now >= end_time) or (symbol in self.config.exit_config)
                        or market_price >= self.config.strategy.stop_loss_price.get(symbol)):

                    del self.config.strategy.positions[symbol]
                    level = 0
                    self.create_order(symbol, TradeType.BUY, level)
                    self.position_update(decrement=True)

                elif n < -0.85 and self.config.strategy.levels.get(symbol) == 1:
                    self.config.strategy.levels[symbol] = 2
                    self.create_order(symbol, TradeType.SELL, self.config.strategy.levels.get(symbol))

                elif (-0.85 < n < -0.6) and self.config.strategy.levels.get(symbol) == 1:
                    self.config.strategy.target_price[symbol] = self.config.strategy.entry_prices.get(symbol) * (
                            1 - (0.35 * stock_sigma / 100))
                    self.logger().info(
                        f'Target Price is updated for {symbol} to {self.config.strategy.target_price.get(symbol)}')
                    self.close_execution_by(CloseType.TAKE_PROFIT)

                elif (-0.65 < n < -0.4) and self.config.strategy.levels.get(symbol) == 3:
                    self.config.strategy.target_price[symbol] = self.config.strategy.entry_prices.get(symbol) * (
                            1 - (0 * stock_sigma / 100))
                    self.logger().info(
                        f'Target Price is updated for {symbol} to {self.config.strategy.target_price.get(symbol)}')
                    self.close_execution_by(CloseType.TAKE_PROFIT)

                elif n > 0.5:
                    self.config.strategy.stop_loss_price[symbol] = self.config.strategy.entry_prices.get(symbol) * (
                            1 - (0.35 * stock_sigma / 100))
                    self.logger().info(
                        f'Stop Loss Price is updated for {symbol} to {self.config.strategy.stop_loss_price.get(symbol)}')
                    self.close_execution_by(CloseType.STOP_LOSS)
                else:
                    self.close_execution_by(CloseType.EXPIRED)
            else:
                self.close_execution_by(CloseType.EXPIRED)
        except Exception as e:
            self.logger().error(f"Exception Occurs in checking exit process data for symbol {symbol} because {e}")
            self.close_execution_by(CloseType.FAILED)

    def process_order_created_event(self,
                                    event_tag: int,
                                    market: ConnectorBase,
                                    event: Union[BuyOrderCreatedEvent, SellOrderCreatedEvent]):
        """
        This method is responsible for processing the order created event. Here we will add the InFlightOrder to the
        active orders list.
        """
        all_orders = self._open_orders + self._close_orders
        active_order = next((order for order in all_orders if order.order_id == event.order_id), None)
        if active_order:
            in_flight_order = self.get_in_flight_order(self.config.connector_name, event.order_id)
            if in_flight_order:
                active_order.order = in_flight_order

    def process_order_failed_event(self,
                                   event_tag: int,
                                   market: ConnectorBase,
                                   event: MarketOrderFailureEvent):
        """
        This method is responsible for processing the order failed event. Here we will add the InFlightOrder to the
        failed orders list.
        """
        open_order = next((order for order in self._open_orders if order.order_id == event.order_id), None)
        if open_order:
            self._failed_orders.append(open_order)
            self._open_orders.remove(open_order)
            self.logger().error(f"Order {self.config.symbol} failed.")
        close_order = next((order for order in self._close_orders if order.order_id == event.order_id), None)
        if close_order:
            self._failed_orders.append(close_order)
            self._close_orders.remove(close_order)
            self.logger().error(f"Order {self.config.symbol} failed.")
        self.close_execution_by(CloseType.FAILED)

    def process_order_filled_event(self, event_tag: int, market: ConnectorBase, event: OrderFilledEvent):
        """
        This method is responsible for processing the order filled event. Here we will update the value of
        """
        symbol = event.trading_pair
        underlying = self.config.strategy.underlying.get(symbol)
        stock_sigma = float(self.config.strategy.sigma.get(underlying))
        price = float(event.price)
        trade_type = event.trade_type

        if self.config.strategy.levels.get(symbol) == 1:
            self.config.strategy.entry_prices[symbol] = price
        elif self.config.strategy.levels.get(symbol) == 2:
            prev_price = self.config.strategy.entry_prices.get(symbol)
            self.config.strategy.entry_prices[symbol] = (prev_price + (2 * price)) / 3
            self.config.strategy.levels[symbol] = 3
        else:
            del self.config.strategy.levels[symbol]

        if trade_type == TradeType.BUY and symbol in self.config.strategy.positions:

            if self.config.strategy.levels.get(symbol) == 1:
                self.config.strategy.target_price[symbol] = self.config.strategy.entry_prices.get(symbol) * (
                            1 + (0.55 * stock_sigma / 100))
                self.config.strategy.stop_loss_price[symbol] = self.config.strategy.entry_prices.get(symbol) * (
                            1 - (1.2 * stock_sigma / 100))
            elif self.config.strategy.levels.get(symbol) == 3:
                self.config.strategy.target_price[symbol] = self.config.strategy.entry_prices.get(symbol) * (
                            1 + (0.3 * stock_sigma / 100))

        elif trade_type == TradeType.SELL and symbol in self.config.strategy.positions:
            if self.config.strategy.levels.get(symbol) == 1:
                self.config.strategy.target_price[symbol] = self.config.strategy.entry_prices.get(symbol) * (
                            1 - (0.6 * stock_sigma / 100))
                self.config.strategy.stop_loss_price[symbol] = self.config.strategy.entry_prices.get(symbol) * (
                            1 + (1.2 * stock_sigma / 100))
            elif self.config.strategy.levels.get(symbol) == 3:
                self.config.strategy.target_price[symbol] = self.config.strategy.entry_prices.get(symbol) * (
                            1 - (0.3 * stock_sigma / 100))
        self.logger().info(f"Order filled at price for {symbol}: {price}")
        self.logger().info(f"Target Price is set for {symbol} at {self.config.strategy.target_price.get(symbol)}")
        self.logger().info(f"Stop Loss Price is set for {symbol} at {self.config.strategy.stop_loss_price.get(symbol)}")
        self.close_execution_by(CloseType.COMPLETED)

    def cancel_open_orders(self):
        for tracked_order in self._open_orders:
            if tracked_order.order and tracked_order.order.is_open:
                self._strategy.cancel(connector_name=self.config.connector_name, trading_pair=self.config.trading_pair,
                                      order_id=tracked_order.order_id)

    def get_custom_info(self) -> Dict:
        return {}

    def validate_sufficient_balance(self):
        pass
