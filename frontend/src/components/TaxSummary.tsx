import type { TaxAnalysis } from "../services/api";

type Props = {
  tax: TaxAnalysis;
};

export function TaxSummary({ tax }: Props) {
  return (
    <div className="card">
      <h3>Tax Summary</h3>
      {tax.realized_data_available === false ? (
        <p className="error">Realized LTCG data not synced from broker trade history yet; remaining exemption may be overstated.</p>
      ) : null}
      <p>Realized LTCG: {tax.total_ltcg_realized.toFixed(2)}</p>
      <p>Unrealized LTCG (Current): {tax.total_ltcg_unrealized.toFixed(2)}</p>
      <p>Unrealized LTCG (By FY End): {(tax.total_ltcg_unrealized_by_fy_end ?? tax.total_ltcg_unrealized).toFixed(2)}</p>
      <p>Unrealized LTCG (Stocks/ETFs, Current): {tax.equity_ltcg_unrealized.toFixed(2)}</p>
      <p>Unrealized LTCG (Stocks/ETFs, By FY End): {(tax.equity_ltcg_unrealized_by_fy_end ?? tax.equity_ltcg_unrealized).toFixed(2)}</p>
      <p>Unrealized LTCG (Mutual Funds, Current): {tax.mf_ltcg_unrealized.toFixed(2)}</p>
      <p>Unrealized LTCG (Mutual Funds, By FY End): {(tax.mf_ltcg_unrealized_by_fy_end ?? tax.mf_ltcg_unrealized).toFixed(2)}</p>
      <p>Remaining Exemption (Current): {tax.remaining_tax_free_ltcg.toFixed(2)}</p>
      <p>Harvestable Gains (Current): {tax.harvestable_gains.toFixed(2)}</p>
      <p>Harvestable Gains (Max by FY End): {(tax.harvestable_gains_by_fy_end ?? tax.harvestable_gains).toFixed(2)}</p>
    </div>
  );
}
