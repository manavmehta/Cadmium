import type { HarvestRecommendation, TaxAnalysis } from "../services/api";
import { HarvestRecommendations } from "../components/HarvestRecommendations";
import { TaxSummary } from "../components/TaxSummary";

type Props = {
  tax: TaxAnalysis;
  recs: HarvestRecommendation;
};

export function TaxAnalysisPage({ tax, recs }: Props) {
  return (
    <section className="grid two">
      <TaxSummary tax={tax} />
      <HarvestRecommendations recs={recs} />
    </section>
  );
}
