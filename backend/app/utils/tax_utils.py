from dataclasses import dataclass

LTCG_EXEMPTION_LIMIT = 125000.0


@dataclass(frozen=True)
class AssetTaxRule:
    ltcg_days_threshold: int
    exemption_eligible: bool


ASSET_TAX_RULES: dict[str, AssetTaxRule] = {
    # Listed equity shares
    "stock": AssetTaxRule(ltcg_days_threshold=365, exemption_eligible=True),
    # Equity ETF treatment aligned with listed equity for this engine
    "etf": AssetTaxRule(ltcg_days_threshold=365, exemption_eligible=True),
    # Mutual funds tracked separately with distinct tax treatment, but LT cutover at 365 days.
    "mf": AssetTaxRule(ltcg_days_threshold=365, exemption_eligible=True),
}


def normalize_asset_type(asset_type: str | None) -> str:
    raw = (asset_type or "").strip().lower()
    if raw in ASSET_TAX_RULES:
        return raw
    if raw == "equity":
        return "stock"
    return "stock"
