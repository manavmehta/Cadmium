import type { TaxAnalysis } from "../services/api";

type Props = {
  tax: TaxAnalysis;
};

export function TaxSummary({ tax }: Props) {
  return (
    <div className="card">
      <h3>Tax Summary</h3>
      <p>Realized LTCG: {tax.total_ltcg_realized.toFixed(2)}</p>
      <p>Unrealized LTCG: {tax.total_ltcg_unrealized.toFixed(2)}</p>
      <p>Unrealized LTCG (Stocks/ETFs): {tax.equity_ltcg_unrealized.toFixed(2)}</p>
      <p>Unrealized LTCG (Mutual Funds): {tax.mf_ltcg_unrealized.toFixed(2)}</p>
      <p>Remaining Exemption: {tax.remaining_tax_free_ltcg.toFixed(2)}</p>
      <p>Harvestable Gains: {tax.harvestable_gains.toFixed(2)}</p>
    </div>
  );
}
