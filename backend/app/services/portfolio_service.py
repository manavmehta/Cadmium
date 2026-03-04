from collections import defaultdict
from datetime import timedelta

from app.models.holding import Holding
from app.schemas.api_schemas import (
    BrokerBreakdownSummary,
    BrokerSymbolBreakdown,
    HoldingOut,
    PortfolioBrokerBreakdown,
    PortfolioSummaryOut,
)
from app.utils.date_utils import holding_period_days
from app.utils.tax_utils import ASSET_TAX_RULES, normalize_asset_type


class PortfolioService:
    @staticmethod
    def _market_value(holding: Holding) -> float:
        return holding.quantity * holding.current_price

    @staticmethod
    def _unrealized_gain(holding: Holding) -> float:
        return holding.quantity * (holding.current_price - holding.average_buy_price)

    @classmethod
    def _is_lt(cls, holding: Holding) -> bool:
        asset_type = normalize_asset_type(holding.asset_type)
        threshold = ASSET_TAX_RULES[asset_type].ltcg_days_threshold
        return holding_period_days(holding.buy_date) > threshold

    @classmethod
    def _next_lt_date(cls, holding: Holding):
        asset_type = normalize_asset_type(holding.asset_type)
        threshold = ASSET_TAX_RULES[asset_type].ltcg_days_threshold
        if cls._is_lt(holding):
            return None
        return holding.buy_date + timedelta(days=threshold + 1)

    @classmethod
    def to_holding_out(cls, holding: Holding) -> HoldingOut:
        is_lt = cls._is_lt(holding)
        return HoldingOut(
            id=holding.id,
            symbol=holding.symbol,
            isin=holding.isin,
            broker=holding.broker,
            quantity=holding.quantity,
            average_buy_price=holding.average_buy_price,
            buy_date=holding.buy_date,
            current_price=holding.current_price,
            asset_type=normalize_asset_type(holding.asset_type),
            market_value=cls._market_value(holding),
            unrealized_gain=cls._unrealized_gain(holding),
            holding_period_days=holding_period_days(holding.buy_date),
            lt_qty=holding.quantity if is_lt else 0.0,
            st_qty=0.0 if is_lt else holding.quantity,
            next_lt_date=cls._next_lt_date(holding),
        )

    @classmethod
    def holdings_aggregated(cls, lots: list[Holding]) -> list[HoldingOut]:
        grouped: dict[tuple[str, str, str, str], list[Holding]] = defaultdict(list)
        for lot in lots:
            key = (lot.broker, lot.symbol, lot.isin, normalize_asset_type(lot.asset_type))
            grouped[key].append(lot)

        out: list[HoldingOut] = []
        pseudo_id = 1
        for (broker, symbol, isin, asset_type), rows in grouped.items():
            qty = sum(r.quantity for r in rows)
            current_price = rows[-1].current_price
            total_cost = sum(r.quantity * r.average_buy_price for r in rows)
            avg = (total_cost / qty) if qty > 0 else 0.0
            market_value = sum(cls._market_value(r) for r in rows)
            unrealized_gain = sum(cls._unrealized_gain(r) for r in rows)
            lt_qty = sum(r.quantity for r in rows if cls._is_lt(r))
            st_qty = qty - lt_qty
            next_lt_dates = [cls._next_lt_date(r) for r in rows if cls._next_lt_date(r) is not None]
            next_lt_date = min(next_lt_dates) if next_lt_dates else None
            earliest_buy = min(r.buy_date for r in rows)

            out.append(
                HoldingOut(
                    id=pseudo_id,
                    symbol=symbol,
                    isin=isin,
                    broker=broker,
                    quantity=qty,
                    average_buy_price=round(avg, 6),
                    buy_date=earliest_buy,
                    current_price=current_price,
                    asset_type=asset_type,
                    market_value=round(market_value, 2),
                    unrealized_gain=round(unrealized_gain, 2),
                    holding_period_days=holding_period_days(earliest_buy),
                    lt_qty=round(lt_qty, 6),
                    st_qty=round(st_qty, 6),
                    next_lt_date=next_lt_date,
                )
            )
            pseudo_id += 1

        out.sort(key=lambda h: (h.broker, h.symbol))
        return out

    @classmethod
    def summarize(cls, lots: list[Holding]) -> PortfolioSummaryOut:
        total_value = 0.0
        total_unrealized_gain = 0.0
        ltcg_eligible_value = 0.0
        stcg_value = 0.0
        by_broker: dict[str, float] = {}

        for lot in lots:
            mv = cls._market_value(lot)
            ug = cls._unrealized_gain(lot)
            total_value += mv
            total_unrealized_gain += ug
            by_broker[lot.broker] = by_broker.get(lot.broker, 0.0) + mv
            if cls._is_lt(lot):
                ltcg_eligible_value += mv
            else:
                stcg_value += mv

        return PortfolioSummaryOut(
            total_value=round(total_value, 2),
            total_unrealized_gain=round(total_unrealized_gain, 2),
            ltcg_eligible_value=round(ltcg_eligible_value, 2),
            stcg_value=round(stcg_value, 2),
            by_broker={k: round(v, 2) for k, v in by_broker.items()},
        )

    @classmethod
    def broker_breakdown(cls, lots: list[Holding]) -> list[PortfolioBrokerBreakdown]:
        by_broker: dict[str, list[Holding]] = defaultdict(list)
        for lot in lots:
            by_broker[lot.broker].append(lot)

        result: list[PortfolioBrokerBreakdown] = []
        for broker, broker_lots in sorted(by_broker.items()):
            symbol_rows = cls.holdings_aggregated(broker_lots)
            symbols = [
                BrokerSymbolBreakdown(
                    symbol=h.symbol,
                    isin=h.isin,
                    asset_type=h.asset_type,
                    lt_qty=h.lt_qty,
                    st_qty=h.st_qty,
                    lt_value=round(h.lt_qty * h.current_price, 2),
                    st_value=round(h.st_qty * h.current_price, 2),
                    next_lt_date=h.next_lt_date,
                )
                for h in symbol_rows
            ]

            lt_value = sum(s.lt_value for s in symbols)
            st_value = sum(s.st_value for s in symbols)
            total_value = lt_value + st_value
            unrealized_gain = sum(cls._unrealized_gain(lot) for lot in broker_lots)
            lt_positions = sum(1 for s in symbols if s.lt_qty > 0)
            st_positions = sum(1 for s in symbols if s.st_qty > 0)

            result.append(
                PortfolioBrokerBreakdown(
                    broker=broker,
                    summary=BrokerBreakdownSummary(
                        total_value=round(total_value, 2),
                        lt_value=round(lt_value, 2),
                        st_value=round(st_value, 2),
                        unrealized_gain=round(unrealized_gain, 2),
                        lt_positions=lt_positions,
                        st_positions=st_positions,
                    ),
                    symbols=symbols,
                )
            )

        return result
