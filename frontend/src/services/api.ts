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
  lt_bookable_now_net?: number;
  lt_bookable_max_by_fy_end_net?: number;
  lt_unrealized_profit_net?: number;
  st_unrealized_profit_net?: number;
  lt_bookable_gain_positive?: number;
  st_bookable_gain_positive?: number;
  by_broker: Record<string, number>;
};

export type TaxAnalysis = {
  realized_data_available?: boolean;
  total_ltcg_realized: number;
  total_ltcg_unrealized: number;
  total_ltcg_unrealized_by_fy_end?: number;
  remaining_tax_free_ltcg: number;
  harvestable_gains: number;
  harvestable_gains_by_fy_end?: number;
  equity_ltcg_unrealized: number;
  mf_ltcg_unrealized: number;
  equity_ltcg_unrealized_by_fy_end?: number;
  mf_ltcg_unrealized_by_fy_end?: number;
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
  upstream_error_code?: string | null;
  lot_refresh_success?: boolean;
  price_refresh_success?: boolean;
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
  lt_profit_net?: number;
  st_profit_net?: number;
  st_profit_turns_lt_by_fy_end?: number;
  st_profit_beyond_fy_end?: number;
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
    lt_bookable_now_net?: number;
    lt_bookable_max_by_fy_end_net?: number;
    st_bookable_now_net?: number;
    st_bookable_beyond_fy_end_net?: number;
    lt_unrealized_profit_net?: number;
    st_unrealized_profit_net?: number;
    lt_bookable_gain_positive?: number;
    st_bookable_gain_positive?: number;
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
