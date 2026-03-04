from datetime import date

from pydantic import BaseModel, Field


class HoldingOut(BaseModel):
    id: int
    symbol: str
    isin: str
    broker: str
    quantity: float
    average_buy_price: float
    buy_date: date
    current_price: float
    asset_type: str
    market_value: float
    unrealized_gain: float
    holding_period_days: int
    lt_qty: float = 0.0
    st_qty: float = 0.0
    next_lt_date: date | None = None


class PortfolioSummaryOut(BaseModel):
    total_value: float
    total_unrealized_gain: float
    ltcg_eligible_value: float
    stcg_value: float
    lt_bookable_now_net: float = 0.0
    lt_bookable_max_by_fy_end_net: float = 0.0
    lt_unrealized_profit_net: float = 0.0
    st_unrealized_profit_net: float = 0.0
    lt_bookable_gain_positive: float = 0.0
    st_bookable_gain_positive: float = 0.0
    by_broker: dict[str, float]


class TaxAnalysisOut(BaseModel):
    realized_data_available: bool = False
    total_ltcg_realized: float
    total_ltcg_unrealized: float
    total_ltcg_unrealized_by_fy_end: float = 0.0
    remaining_tax_free_ltcg: float
    harvestable_gains: float
    harvestable_gains_by_fy_end: float = 0.0
    equity_ltcg_unrealized: float = 0.0
    mf_ltcg_unrealized: float = 0.0
    equity_ltcg_unrealized_by_fy_end: float = 0.0
    mf_ltcg_unrealized_by_fy_end: float = 0.0


class HarvestRecommendationItem(BaseModel):
    symbol: str
    broker: str
    sell_quantity: int
    expected_gain: float
    reasoning: str


class HarvestRecommendationOut(BaseModel):
    remaining_exemption: float
    recommendations: list[HarvestRecommendationItem]


class SyncResponse(BaseModel):
    started: bool
    message: str


class BrokerActionResponse(BaseModel):
    broker: str
    success: bool
    message: str
    holdings_synced: int = 0
    lots_synced: int = 0
    data_quality: str = "unreliable"
    error_code: str | None = None
    upstream_error_code: str | None = None
    lot_refresh_success: bool = False
    price_refresh_success: bool = False


class BrokerSyncResult(BaseModel):
    broker: str
    success: bool
    holdings_synced: int = 0
    lots_synced: int = 0
    data_quality: str = "unreliable"
    message: str
    error_code: str | None = None
    upstream_error_code: str | None = None
    lot_refresh_success: bool = False
    price_refresh_success: bool = False


class BrokerStatusItem(BaseModel):
    broker: str
    connected: bool
    session_file: str


class BrokerSymbolBreakdown(BaseModel):
    symbol: str
    isin: str
    asset_type: str
    lt_qty: float
    st_qty: float
    lt_value: float
    st_value: float
    next_lt_date: date | None = None
    lt_profit_net: float = 0.0
    st_profit_net: float = 0.0
    st_profit_turns_lt_by_fy_end: float = 0.0
    st_profit_beyond_fy_end: float = 0.0


class BrokerBreakdownSummary(BaseModel):
    total_value: float
    lt_value: float
    st_value: float
    unrealized_gain: float
    lt_positions: int
    st_positions: int
    lt_bookable_now_net: float = 0.0
    lt_bookable_max_by_fy_end_net: float = 0.0
    st_bookable_now_net: float = 0.0
    st_bookable_beyond_fy_end_net: float = 0.0
    lt_unrealized_profit_net: float = 0.0
    st_unrealized_profit_net: float = 0.0
    lt_bookable_gain_positive: float = 0.0
    st_bookable_gain_positive: float = 0.0


class PortfolioBrokerBreakdown(BaseModel):
    broker: str
    summary: BrokerBreakdownSummary
    symbols: list[BrokerSymbolBreakdown]


class AIRequest(BaseModel):
    holdings: list[dict]
    remaining_ltcg_allowance: float = Field(default=125000)


class AIResponse(BaseModel):
    stocks_to_sell: list[str]
    sell_quantities: list[int]
    expected_gain: float
    reasoning: str
