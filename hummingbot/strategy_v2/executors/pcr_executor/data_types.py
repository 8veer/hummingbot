from decimal import Decimal
from typing import Optional, List, Any
from datetime import datetime, timezone, timedelta

from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.executors.data_types import ExecutorConfigBase
from hummingbot.strategy_v2.executors.position_executor.data_types import TrailingStop


class PCRExecutorConfig(ExecutorConfigBase):
    type = "pcr_executor"
    connector_name: str
    trading_pair: str
    symbol: str
    underlying: str
    side: TradeType
    strategy: Any = None
    pos: str
    process_exit: bool
    order_placed: bool = False
    exit_config: List[str] = None
    market_price: float
    stock_sigma: float
    stop_time: datetime = datetime.now(timezone(timedelta(hours=5.5))).replace(hour=15, minute=25)
    level_id: Optional[str] = None
    take_profit: Optional[Decimal] = None
    stop_loss: Optional[Decimal] = None
    trailing_stop: Optional[TrailingStop] = None
    time_limit: Optional[int] = None
    con_id: Optional[str] = None
    future: Optional[str] = None
