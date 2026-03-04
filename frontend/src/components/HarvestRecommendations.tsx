import type { HarvestRecommendation } from "../services/api";

type Props = {
  recs: HarvestRecommendation;
};

export function HarvestRecommendations({ recs }: Props) {
  return (
    <div className="card">
      <h3>Harvest Recommendations</h3>
      <p>Remaining Exemption: {recs.remaining_exemption.toFixed(2)}</p>
      {recs.recommendations.length === 0 && <p>No eligible recommendations yet.</p>}
      <ul>
        {recs.recommendations.map((r) => (
          <li key={`${r.symbol}-${r.broker}`}>
            {r.symbol} ({r.broker}): sell {r.sell_quantity}, expected gain {r.expected_gain.toFixed(2)}
          </li>
        ))}
      </ul>
    </div>
  );
}
