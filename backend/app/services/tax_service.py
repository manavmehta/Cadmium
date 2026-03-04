from datetime import date, timedelta

from app.models.holding import Holding
from app.models.transaction import Transaction
from app.schemas.api_schemas import HarvestRecommendationItem, HarvestRecommendationOut, TaxAnalysisOut
from app.utils.date_utils import holding_period_days
from app.utils.tax_utils import ASSET_TAX_RULES, LTCG_EXEMPTION_LIMIT, normalize_asset_type


class TaxService:
    @staticmethod
    def _gain_per_share(holding: Holding) -> float:
        return holding.current_price - holding.average_buy_price

    @classmethod
    def analyze(cls, holdings: list[Holding], transactions: list[Transaction]) -> TaxAnalysisOut:
        today = date.today()
        fy_end_year = today.year if today <= date(today.year, 3, 31) else today.year + 1
        fy_end = date(fy_end_year, 3, 31)

        total_ltcg_realized = 0.0
        sell_count = 0
        for tx in transactions:
            if tx.transaction_type.upper() != "SELL":
                continue
            sell_count += 1
            # Existing transaction model does not include buy-cost linkage.
            # Treat this as externally supplied realized LTCG amount basis (best effort).
            if holding_period_days(tx.date) > 0:
                total_ltcg_realized += tx.quantity * tx.price

        total_ltcg_unrealized = 0.0
        total_ltcg_unrealized_by_fy_end = 0.0
        equity_ltcg_unrealized = 0.0
        mf_ltcg_unrealized = 0.0
        equity_ltcg_unrealized_by_fy_end = 0.0
        mf_ltcg_unrealized_by_fy_end = 0.0
        lt_positive_now = 0.0
        lt_negative_now = 0.0
        lt_positive_by_fy_end = 0.0
        lt_negative_by_fy_end = 0.0

        for holding in holdings:
            days = holding_period_days(holding.buy_date)
            asset_type = normalize_asset_type(holding.asset_type)
            rule = ASSET_TAX_RULES[asset_type]
            gain_net = holding.quantity * cls._gain_per_share(holding)
            is_lt_now = days > rule.ltcg_days_threshold
            turns_lt_by_fy_end = (holding.buy_date + timedelta(days=rule.ltcg_days_threshold + 1)) <= fy_end
            is_lt_by_fy_end = is_lt_now or turns_lt_by_fy_end

            if is_lt_now:
                total_ltcg_unrealized += gain_net
                if asset_type == "mf":
                    mf_ltcg_unrealized += gain_net
                else:
                    equity_ltcg_unrealized += gain_net
                if gain_net >= 0:
                    lt_positive_now += gain_net
                else:
                    lt_negative_now += -gain_net

            if is_lt_by_fy_end:
                total_ltcg_unrealized_by_fy_end += gain_net
                if asset_type == "mf":
                    mf_ltcg_unrealized_by_fy_end += gain_net
                else:
                    equity_ltcg_unrealized_by_fy_end += gain_net
                if gain_net >= 0:
                    lt_positive_by_fy_end += gain_net
                else:
                    lt_negative_by_fy_end += -gain_net

        remaining = max(0.0, LTCG_EXEMPTION_LIMIT - total_ltcg_realized)
        harvestable_now = min(lt_positive_now, remaining + lt_negative_now)
        harvestable_by_fy_end = min(lt_positive_by_fy_end, remaining + lt_negative_by_fy_end)

        return TaxAnalysisOut(
            realized_data_available=sell_count > 0,
            total_ltcg_realized=round(total_ltcg_realized, 2),
            total_ltcg_unrealized=round(total_ltcg_unrealized, 2),
            total_ltcg_unrealized_by_fy_end=round(total_ltcg_unrealized_by_fy_end, 2),
            remaining_tax_free_ltcg=round(remaining, 2),
            harvestable_gains=round(harvestable_now, 2),
            harvestable_gains_by_fy_end=round(harvestable_by_fy_end, 2),
            equity_ltcg_unrealized=round(equity_ltcg_unrealized, 2),
            mf_ltcg_unrealized=round(mf_ltcg_unrealized, 2),
            equity_ltcg_unrealized_by_fy_end=round(equity_ltcg_unrealized_by_fy_end, 2),
            mf_ltcg_unrealized_by_fy_end=round(mf_ltcg_unrealized_by_fy_end, 2),
        )

    @classmethod
    def recommend_harvest(cls, holdings: list[Holding], remaining_exemption: float) -> HarvestRecommendationOut:
        ordered = sorted(holdings, key=lambda h: cls._gain_per_share(h), reverse=True)
        recommendations: list[HarvestRecommendationItem] = []
        remaining = remaining_exemption

        for holding in ordered:
            if remaining <= 0:
                break

            days = holding_period_days(holding.buy_date)
            asset_type = normalize_asset_type(holding.asset_type)
            rule = ASSET_TAX_RULES[asset_type]
            if not rule.exemption_eligible or days <= rule.ltcg_days_threshold:
                continue

            gain_per_share = cls._gain_per_share(holding)
            if gain_per_share <= 0:
                continue

            max_qty = int(min(holding.quantity, remaining // gain_per_share))
            if max_qty <= 0:
                continue

            expected_gain = round(max_qty * gain_per_share, 2)
            remaining -= expected_gain
            recommendations.append(
                HarvestRecommendationItem(
                    symbol=holding.symbol,
                    broker=holding.broker,
                    sell_quantity=max_qty,
                    expected_gain=expected_gain,
                    reasoning="LTCG-eligible position with positive unrealized gain.",
                )
            )

        return HarvestRecommendationOut(
            remaining_exemption=round(remaining_exemption, 2),
            recommendations=recommendations,
        )
