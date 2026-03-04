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
        total_ltcg_realized = 0.0
        for tx in transactions:
            if tx.transaction_type.upper() != "SELL":
                continue
            
            if holding_period_days(tx.date) > 0:
                total_ltcg_realized += tx.quantity * tx.price

        total_ltcg_unrealized = 0.0
        equity_ltcg_unrealized = 0.0
        mf_ltcg_unrealized = 0.0
        for holding in holdings:
            days = holding_period_days(holding.buy_date)
            asset_type = normalize_asset_type(holding.asset_type)
            rule = ASSET_TAX_RULES[asset_type]
            gain = max(0.0, holding.quantity * cls._gain_per_share(holding))
            if days > rule.ltcg_days_threshold:
                total_ltcg_unrealized += gain
                if rule.exemption_eligible:
                    equity_ltcg_unrealized += gain
                elif asset_type == "mf":
                    mf_ltcg_unrealized += gain

        remaining = max(0.0, LTCG_EXEMPTION_LIMIT - total_ltcg_realized)
        harvestable = min(remaining, equity_ltcg_unrealized)

        return TaxAnalysisOut(
            total_ltcg_realized=round(total_ltcg_realized, 2),
            total_ltcg_unrealized=round(total_ltcg_unrealized, 2),
            remaining_tax_free_ltcg=round(remaining, 2),
            harvestable_gains=round(harvestable, 2),
            equity_ltcg_unrealized=round(equity_ltcg_unrealized, 2),
            mf_ltcg_unrealized=round(mf_ltcg_unrealized, 2),
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
