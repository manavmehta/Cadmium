const API_BASE = import.meta.env.VITE_API_BASE ?? "/api";

export type Holding = {
  id: number;
  symbol: string;
  broker: string;
  buy_date: string;
  asset_type: "stock" | "etf" | "mf" | string;
  quantity: number;
  average_buy_price: number;
  current_price: number;
  market_value: number;
  unrealized_gain: number;
  holding_period_days: number;
  lt_qty: number;
  st_qty: number;
  next_lt_date?: string | null;
};

export type PortfolioSummary = {
  total_value: number;
  total_unrealized_gain: number;
  ltcg_eligible_value: number;
  stcg_value: number;
  by_broker: Record<string, number>;
};

export type TaxAnalysis = {
  total_ltcg_realized: number;
  total_ltcg_unrealized: number;
  remaining_tax_free_ltcg: number;
  harvestable_gains: number;
  equity_ltcg_unrealized: number;
  mf_ltcg_unrealized: number;
};

export type HarvestRecommendation = {
  remaining_exemption: number;
  recommendations: Array<{
    symbol: string;
    broker: string;
    sell_quantity: number;
    expected_gain: number;
    reasoning: string;
  }>;
};

export type BrokerStatus = {
  broker: string;
  connected: boolean;
  session_file: string;
};

export type BrokerActionResponse = {
  broker: string;
  success: boolean;
  message: string;
  holdings_synced: number;
  lots_synced: number;
  data_quality: "reliable" | "unreliable" | string;
  error_code?: string | null;
};

export type BrokerSymbolBreakdown = {
  symbol: string;
  isin: string;
  asset_type: string;
  lt_qty: number;
  st_qty: number;
  lt_value: number;
  st_value: number;
  next_lt_date?: string | null;
};

export type PortfolioBrokerBreakdown = {
  broker: string;
  summary: {
    total_value: number;
    lt_value: number;
    st_value: number;
    unrealized_gain: number;
    lt_positions: number;
    st_positions: number;
  };
  symbols: BrokerSymbolBreakdown[];
};

async function fetchJSON<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, init);
  if (!res.ok) {
    let detail = "";
    try {
      const body = (await res.json()) as { detail?: string };
      if (body?.detail) {
        detail = body.detail;
      }
    } catch {
      detail = "";
    }
    throw new Error(detail ? `Request failed: ${res.status} - ${detail}` : `Request failed: ${res.status}`);
  }
  return res.json() as Promise<T>;
}

export const api = {
  getSummary: () => fetchJSON<PortfolioSummary>(`${API_BASE}/portfolio/summary`),
  getHoldings: () => fetchJSON<Holding[]>(`${API_BASE}/portfolio/holdings`),
  getTax: () => fetchJSON<TaxAnalysis>(`${API_BASE}/tax/analysis`),
  getHarvest: () => fetchJSON<HarvestRecommendation>(`${API_BASE}/tax/harvest`),
  getBrokerBreakdown: () => fetchJSON<PortfolioBrokerBreakdown[]>(`${API_BASE}/portfolio/broker-breakdown`),
  getBrokerStatus: () => fetchJSON<BrokerStatus[]>(`${API_BASE}/brokers/status`),
  loginBroker: (broker: string, waitSeconds = 600) =>
    fetchJSON<BrokerActionResponse>(`${API_BASE}/brokers/${broker}/login?wait_seconds=${waitSeconds}`, {
      method: "POST"
    }),
  syncBroker: (broker: string) =>
    fetchJSON<BrokerActionResponse>(`${API_BASE}/brokers/${broker}/sync`, { method: "POST" })
};
