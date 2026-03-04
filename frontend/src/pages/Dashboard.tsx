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

function toAssetLabel(assetType: string) {
  return assetType === "mf" ? "MF" : "EQ";
}

function BrokerSection({ row, fyEnd }: { row: PortfolioBrokerBreakdown; fyEnd: Date }) {
  const ltSymbols = row.symbols.filter((s) => (s.lt_profit_net ?? 0) !== 0 || s.lt_qty > 0);
  const stTurnsLtByFyEnd = row.symbols.filter(
    (s) => (s.st_profit_turns_lt_by_fy_end ?? 0) !== 0 && s.next_lt_date
  );
  const stBeyondFyEnd = row.symbols.filter((s) => (s.st_profit_beyond_fy_end ?? 0) !== 0);
  const hasData = row.symbols.length > 0;
  const ltBucketNet = ltSymbols.reduce((acc, s) => acc + (s.lt_profit_net ?? 0), 0);
  const ltBeforeFyEndBucketNet = stTurnsLtByFyEnd.reduce(
    (acc, s) => acc + (s.st_profit_turns_lt_by_fy_end ?? 0),
    0
  );
  const stBucketNet = stBeyondFyEnd.reduce((acc, s) => acc + (s.st_profit_beyond_fy_end ?? 0), 0);

  return (
    <section className="card">
      <h3>{row.broker.toUpperCase()} Holdings Summary</h3>
      {!hasData && <p>No reliable lot-level data synced for this broker yet.</p>}

      <div className="grid three">
        <div className="card card-sub">
          <h4>Long-Term Holdings</h4>
          <p>{ltSymbols.length} positions</p>
          <p>Net Bookable P/L: {ltBucketNet.toFixed(2)}</p>
          {ltSymbols.length === 0 ? (
            <p>No long-term positions currently.</p>
          ) : (
            <ul>
              {ltSymbols.map((s) => (
                <li key={`lt-${row.broker}-${s.symbol}`}>
                  {s.symbol} ({toAssetLabel(s.asset_type)}) | Bookable P/L {(s.lt_profit_net ?? 0).toFixed(2)}
                </li>
              ))}
            </ul>
          )}
        </div>
        <div className="card card-sub">
          <h4>LT Before FY End</h4>
          <p>{stTurnsLtByFyEnd.length} positions</p>
          <p>Net Bookable P/L: {ltBeforeFyEndBucketNet.toFixed(2)}</p>
          {stTurnsLtByFyEnd.length === 0 ? (
            <p>No positions transition to LT by FY end.</p>
          ) : (
            <ul>
              {stTurnsLtByFyEnd.map((s) => (
                <li key={`ltfy-${row.broker}-${s.symbol}`}>
                  {s.symbol} ({toAssetLabel(s.asset_type)}) | Bookable P/L{" "}
                  {(s.st_profit_turns_lt_by_fy_end ?? 0).toFixed(2)} | LT from{" "}
                  {formatDate(new Date(`${s.next_lt_date}T00:00:00`))}
                </li>
              ))}
            </ul>
          )}
        </div>
        <div className="card card-sub">
          <h4>ST Holdings</h4>
          <p>{stBeyondFyEnd.length} positions</p>
          <p>Net Bookable P/L: {stBucketNet.toFixed(2)}</p>
          {stBeyondFyEnd.length === 0 ? (
            <p>No short-term positions beyond FY end.</p>
          ) : (
            <ul>
              {stBeyondFyEnd.map((s) => (
                <li key={`st-${row.broker}-${s.symbol}`}>
                  {s.symbol} ({toAssetLabel(s.asset_type)}) | Bookable P/L{" "}
                  {(s.st_profit_beyond_fy_end ?? 0).toFixed(2)}
                  {s.next_lt_date && new Date(`${s.next_lt_date}T00:00:00`) > fyEnd
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
  const ltNow = summary.lt_bookable_now_net ?? summary.lt_unrealized_profit_net ?? summary.ltcg_eligible_value;
  const ltMaxByFyEnd = summary.lt_bookable_max_by_fy_end_net ?? ltNow;
  const stNet = summary.st_unrealized_profit_net ?? summary.stcg_value;

  return (
    <section className="grid one">
      <div className="card">
        <h2>Aggregate Portfolio Overview (All Brokers)</h2>
        <p>Lot-based classification only. ST positions maturing on/before {formatDate(fyEnd)} show LT date tags.</p>
        <p>Total Value: {summary.total_value.toFixed(2)}</p>
        <p>Total Unrealized Gain: {summary.total_unrealized_gain.toFixed(2)}</p>
        <p>LT Bookable Now (Net): {ltNow.toFixed(2)}</p>
        <p>LT Max Possible by FY End (Net at current LTP): {ltMaxByFyEnd.toFixed(2)}</p>
        <p>ST Bookable P/L (Net): {stNet.toFixed(2)}</p>
        <h3>LTCG Status Across All Brokers</h3>
        {tax.realized_data_available === false ? (
          <p className="error">Realized LTCG from broker trade history is not synced yet; remaining exemption may be overstated.</p>
        ) : null}
        <p>Remaining Tax-Free LTCG (Current): {tax.remaining_tax_free_ltcg.toFixed(2)}</p>
        <p>Harvestable Gains (Current): {tax.harvestable_gains.toFixed(2)}</p>
        <p>Harvestable Gains (Max by FY End): {(tax.harvestable_gains_by_fy_end ?? tax.harvestable_gains).toFixed(2)}</p>
        <p>Unrealized LTCG (Stocks/ETFs, Current): {tax.equity_ltcg_unrealized.toFixed(2)}</p>
        <p>Unrealized LTCG (Stocks/ETFs, By FY End): {(tax.equity_ltcg_unrealized_by_fy_end ?? tax.equity_ltcg_unrealized).toFixed(2)}</p>
        <p>Unrealized LTCG (Mutual Funds, Current): {tax.mf_ltcg_unrealized.toFixed(2)}</p>
        <p>Unrealized LTCG (Mutual Funds, By FY End): {(tax.mf_ltcg_unrealized_by_fy_end ?? tax.mf_ltcg_unrealized).toFixed(2)}</p>
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
