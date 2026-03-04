import type { Holding } from "../services/api";
import { HoldingsTable } from "../components/HoldingsTable";

type Props = {
  holdings: Holding[];
};

export function Portfolio({ holdings }: Props) {
  return <HoldingsTable holdings={holdings} />;
}
