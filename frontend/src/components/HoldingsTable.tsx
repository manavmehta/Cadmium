import type { Holding } from "../services/api";

type Props = {
  holdings: Holding[];
};

export function HoldingsTable({ holdings }: Props) {
  return (
    <div className="card">
      <h3>Holdings</h3>
      <table>
        <thead>
          <tr>
            <th>Symbol</th>
            <th>Broker</th>
            <th>Asset Type</th>
            <th>Qty</th>
            <th>Current Price</th>
            <th>Unrealized Gain</th>
            <th>Days</th>
          </tr>
        </thead>
        <tbody>
          {holdings.map((h) => (
            <tr key={h.id}>
              <td>{h.symbol}</td>
              <td>{h.broker}</td>
              <td>{h.asset_type}</td>
              <td>{h.quantity}</td>
              <td>{h.current_price.toFixed(2)}</td>
              <td className={h.unrealized_gain >= 0 ? "pos" : "neg"}>{h.unrealized_gain.toFixed(2)}</td>
              <td>{h.holding_period_days}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
