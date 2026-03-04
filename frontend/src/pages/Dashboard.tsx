import type { PortfolioBrokerBreakdown, PortfolioSummary, TaxAnalysis } from "../services/api";

type Props = {
  summary: PortfolioSummary;
  tax: TaxAnalysis;
  brokerBreakdown: PortfolioBrokerBreakdown[];
};

function getFyEndDate() {
  const now = new Date();
  return new Date(now.getFullYear(), 2, 31, 23, 59, 59, 999);
}

function formatDate(d: Date) {
  return new Intl.DateTimeFormat("en-IN", { day: "2-digit", month: "short", year: "numeric" }).format(d);
}

function BrokerSection({ row, fyEnd }: { row: PortfolioBrokerBreakdown; fyEnd: Date }) {
  const ltSymbols = row.symbols.filter((s) => s.lt_qty > 0);
  const stSymbols = row.symbols.filter((s) => s.st_qty > 0);

  return (
    <section className="card">
      <h3>{row.broker.toUpperCase()} Holdings Summary</h3>
      <p>Total Value: {row.summary.total_value.toFixed(2)}</p>
      <p>Total Unrealized Gain: {row.summary.unrealized_gain.toFixed(2)}</p>
      <p>LTCG Eligible Value: {row.summary.lt_value.toFixed(2)}</p>
      <p>STCG Value: {row.summary.st_value.toFixed(2)}</p>

      <div className="grid two">
        <div className="card card-sub">
          <h4>Long-Term Holdings</h4>
          <p>{row.summary.lt_positions} positions</p>
          {ltSymbols.length === 0 ? (
            <p>No long-term positions currently.</p>
          ) : (
            <ul>
              {ltSymbols.map((s) => (
                <li key={`lt-${row.broker}-${s.symbol}`}>
                  {s.symbol} ({s.asset_type}) | LT qty {s.lt_qty} | LT value {s.lt_value.toFixed(2)}
                  {s.next_lt_date && new Date(`${s.next_lt_date}T00:00:00`) <= fyEnd
                    ? ` | LT from ${formatDate(new Date(`${s.next_lt_date}T00:00:00`))}`
                    : ""}
                </li>
              ))}
            </ul>
          )}
        </div>
        <div className="card card-sub">
          <h4>Short-Term Holdings</h4>
          <p>{row.summary.st_positions} positions</p>
          {stSymbols.length === 0 ? (
            <p>No short-term positions currently.</p>
          ) : (
            <ul>
              {stSymbols.map((s) => (
                <li key={`st-${row.broker}-${s.symbol}`}>
                  {s.symbol} ({s.asset_type}) | ST qty {s.st_qty} | ST value {s.st_value.toFixed(2)}
                  {s.next_lt_date && new Date(`${s.next_lt_date}T00:00:00`) <= fyEnd
                    ? ` | LT from ${formatDate(new Date(`${s.next_lt_date}T00:00:00`))}`
                    : ""}
                </li>
              ))}
            </ul>
          )}
        </div>
      </div>
    </section>
  );
}

export function Dashboard({ summary, tax, brokerBreakdown }: Props) {
  const fyEnd = getFyEndDate();

  return (
    <section className="grid one">
      <div className="card">
        <h2>Aggregate Portfolio Overview (All Brokers)</h2>
        <p>Lot-based classification only. ST positions maturing on/before {formatDate(fyEnd)} show LT date tags.</p>
        <p>Total Value: {summary.total_value.toFixed(2)}</p>
        <p>Total Unrealized Gain: {summary.total_unrealized_gain.toFixed(2)}</p>
        <p>LTCG Eligible Value: {summary.ltcg_eligible_value.toFixed(2)}</p>
        <p>STCG Value: {summary.stcg_value.toFixed(2)}</p>
        <h3>LTCG Status Across All Brokers</h3>
        <p>Remaining Tax-Free LTCG: {tax.remaining_tax_free_ltcg.toFixed(2)}</p>
        <p>Harvestable Gains: {tax.harvestable_gains.toFixed(2)}</p>
        <p>Unrealized LTCG (Stocks/ETFs): {tax.equity_ltcg_unrealized.toFixed(2)}</p>
        <p>Unrealized LTCG (Mutual Funds): {tax.mf_ltcg_unrealized.toFixed(2)}</p>
      </div>

      {brokerBreakdown
        .slice()
        .sort((a, b) => a.broker.localeCompare(b.broker))
        .map((row) => (
          <BrokerSection key={row.broker} row={row} fyEnd={fyEnd} />
        ))}
    </section>
  );
}
