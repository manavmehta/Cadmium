from collections import defaultdict
from datetime import date, timedelta

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
        today = date.today()
        fy_end_year = today.year if today <= date(today.year, 3, 31) else today.year + 1
        fy_end = date(fy_end_year, 3, 31)

        total_value = 0.0
        total_unrealized_gain = 0.0
        lt_unrealized_profit_net = 0.0
        st_unrealized_profit_net = 0.0
        st_turns_lt_by_fy_end_net = 0.0
        st_turns_lt_by_fy_end_positive = 0.0
        lt_bookable_gain_positive = 0.0
        st_bookable_gain_positive = 0.0
        by_broker: dict[str, float] = {}

        for lot in lots:
            mv = cls._market_value(lot)
            ug = cls._unrealized_gain(lot)
            total_value += mv
            total_unrealized_gain += ug
            by_broker[lot.broker] = by_broker.get(lot.broker, 0.0) + mv
            if cls._is_lt(lot):
                lt_unrealized_profit_net += ug
                lt_bookable_gain_positive += max(0.0, ug)
            else:
                st_unrealized_profit_net += ug
                st_bookable_gain_positive += max(0.0, ug)
                nld = cls._next_lt_date(lot)
                if nld is not None and nld <= fy_end:
                    st_turns_lt_by_fy_end_net += ug
                    st_turns_lt_by_fy_end_positive += max(0.0, ug)

        return PortfolioSummaryOut(
            total_value=round(total_value, 2),
            total_unrealized_gain=round(total_unrealized_gain, 2),
            # Compatibility fields now carry net LT/ST unrealized profit.
            ltcg_eligible_value=round(lt_unrealized_profit_net, 2),
            stcg_value=round(st_unrealized_profit_net, 2),
            lt_bookable_now_net=round(lt_unrealized_profit_net, 2),
            lt_bookable_max_by_fy_end_net=round(lt_unrealized_profit_net + st_turns_lt_by_fy_end_positive, 2),
            lt_unrealized_profit_net=round(lt_unrealized_profit_net, 2),
            st_unrealized_profit_net=round(st_unrealized_profit_net, 2),
            lt_bookable_gain_positive=round(lt_bookable_gain_positive, 2),
            st_bookable_gain_positive=round(st_bookable_gain_positive, 2),
            by_broker={k: round(v, 2) for k, v in by_broker.items()},
        )

    @classmethod
    def broker_breakdown(
        cls, lots: list[Holding], all_brokers: list[str] | None = None
    ) -> list[PortfolioBrokerBreakdown]:
        today = date.today()
        fy_end_year = today.year if today <= date(today.year, 3, 31) else today.year + 1
        fy_end = date(fy_end_year, 3, 31)

        by_broker: dict[str, list[Holding]] = defaultdict(list)
        for lot in lots:
            by_broker[lot.broker].append(lot)

        if all_brokers:
            for broker in all_brokers:
                by_broker.setdefault(broker, [])

        result: list[PortfolioBrokerBreakdown] = []
        for broker, broker_lots in sorted(by_broker.items()):
            grouped: dict[tuple[str, str, str], list[Holding]] = defaultdict(list)
            for lot in broker_lots:
                key = (lot.symbol, lot.isin, normalize_asset_type(lot.asset_type))
                grouped[key].append(lot)

            symbols: list[BrokerSymbolBreakdown] = []
            for (symbol, isin, asset_type), symbol_lots in sorted(grouped.items()):
                lt_qty = 0.0
                st_qty = 0.0
                lt_value = 0.0
                st_value = 0.0
                lt_profit_net = 0.0
                st_profit_net = 0.0
                st_profit_turns_lt_by_fy_end = 0.0
                st_profit_beyond_fy_end = 0.0
                next_lt_dates: list[date] = []

                for lot in symbol_lots:
                    is_lt = cls._is_lt(lot)
                    mv = cls._market_value(lot)
                    ug = cls._unrealized_gain(lot)
                    nld = cls._next_lt_date(lot)
                    if is_lt:
                        lt_qty += lot.quantity
                        lt_value += mv
                        lt_profit_net += ug
                    else:
                        st_qty += lot.quantity
                        st_value += mv
                        st_profit_net += ug
                        if nld is not None:
                            next_lt_dates.append(nld)
                            if nld <= fy_end:
                                st_profit_turns_lt_by_fy_end += ug
                            else:
                                st_profit_beyond_fy_end += ug
                        else:
                            st_profit_beyond_fy_end += ug

                symbols.append(
                    BrokerSymbolBreakdown(
                        symbol=symbol,
                        isin=isin,
                        asset_type=asset_type,
                        lt_qty=round(lt_qty, 6),
                        st_qty=round(st_qty, 6),
                        lt_value=round(lt_value, 2),
                        st_value=round(st_value, 2),
                        next_lt_date=min(next_lt_dates) if next_lt_dates else None,
                        lt_profit_net=round(lt_profit_net, 2),
                        st_profit_net=round(st_profit_net, 2),
                        st_profit_turns_lt_by_fy_end=round(st_profit_turns_lt_by_fy_end, 2),
                        st_profit_beyond_fy_end=round(st_profit_beyond_fy_end, 2),
                    )
                )

            lt_profit_net = sum(cls._unrealized_gain(lot) for lot in broker_lots if cls._is_lt(lot))
            st_profit_net = sum(cls._unrealized_gain(lot) for lot in broker_lots if not cls._is_lt(lot))
            st_turns_lt_by_fy_end_net = 0.0
            st_turns_lt_by_fy_end_positive = 0.0
            for lot in broker_lots:
                if cls._is_lt(lot):
                    continue
                nld = cls._next_lt_date(lot)
                if nld is not None and nld <= fy_end:
                    ug = cls._unrealized_gain(lot)
                    st_turns_lt_by_fy_end_net += ug
                    st_turns_lt_by_fy_end_positive += max(0.0, ug)
            st_beyond_fy_end_net = st_profit_net - st_turns_lt_by_fy_end_net
            lt_bookable_gain_positive = sum(
                max(0.0, cls._unrealized_gain(lot)) for lot in broker_lots if cls._is_lt(lot)
            )
            st_bookable_gain_positive = sum(
                max(0.0, cls._unrealized_gain(lot)) for lot in broker_lots if not cls._is_lt(lot)
            )
            total_value = sum(cls._market_value(lot) for lot in broker_lots)
            unrealized_gain = sum(cls._unrealized_gain(lot) for lot in broker_lots)
            lt_positions = sum(1 for s in symbols if s.lt_qty > 0)
            st_positions = sum(1 for s in symbols if s.st_qty > 0)

            result.append(
                PortfolioBrokerBreakdown(
                    broker=broker,
                    summary=BrokerBreakdownSummary(
                        total_value=round(total_value, 2),
                        # Compatibility fields now carry net LT/ST unrealized profit.
                        lt_value=round(lt_profit_net, 2),
                        st_value=round(st_profit_net, 2),
                        unrealized_gain=round(unrealized_gain, 2),
                        lt_positions=lt_positions,
                        st_positions=st_positions,
                        lt_bookable_now_net=round(lt_profit_net, 2),
                        lt_bookable_max_by_fy_end_net=round(lt_profit_net + st_turns_lt_by_fy_end_positive, 2),
                        st_bookable_now_net=round(st_profit_net, 2),
                        st_bookable_beyond_fy_end_net=round(st_beyond_fy_end_net, 2),
                        lt_unrealized_profit_net=round(lt_profit_net, 2),
                        st_unrealized_profit_net=round(st_profit_net, 2),
                        lt_bookable_gain_positive=round(lt_bookable_gain_positive, 2),
                        st_bookable_gain_positive=round(st_bookable_gain_positive, 2),
                    ),
                    symbols=symbols,
                )
            )

        return result
