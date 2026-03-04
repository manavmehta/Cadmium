import { useEffect, useState } from "react";

import { Dashboard } from "./pages/Dashboard";
import { Portfolio } from "./pages/Portfolio";
import { TaxAnalysisPage } from "./pages/TaxAnalysis";
import {
  api,
  type BrokerActionResponse,
  type BrokerStatus,
  type HarvestRecommendation,
  type Holding,
  type PortfolioBrokerBreakdown,
  type PortfolioSummary,
  type TaxAnalysis
} from "./services/api";

type Tab = "dashboard" | "portfolio" | "tax";

export function App() {
  const [tab, setTab] = useState<Tab>("dashboard");
  const [summary, setSummary] = useState<PortfolioSummary | null>(null);
  const [holdings, setHoldings] = useState<Holding[]>([]);
  const [tax, setTax] = useState<TaxAnalysis | null>(null);
  const [recs, setRecs] = useState<HarvestRecommendation | null>(null);
  const [brokerBreakdown, setBrokerBreakdown] = useState<PortfolioBrokerBreakdown[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [brokers, setBrokers] = useState<BrokerStatus[]>([]);
  const [brokerAction, setBrokerAction] = useState<string | null>(null);
  const [busyBroker, setBusyBroker] = useState<string | null>(null);
  const [hasConnectedBroker, setHasConnectedBroker] = useState(false);
  const [syncStatus, setSyncStatus] = useState<Record<string, { at: string; result: BrokerActionResponse }>>({});

  const clearPortfolioData = () => {
    setSummary(null);
    setHoldings([]);
    setTax(null);
    setRecs(null);
    setBrokerBreakdown([]);
  };

  const loadData = async () => {
    setLoading(true);
    setError(null);
    try {
      const [s, h, t, r, bb] = await Promise.all([
        api.getSummary(),
        api.getHoldings(),
        api.getTax(),
        api.getHarvest(),
        api.getBrokerBreakdown()
      ]);
      setSummary(s);
      setHoldings(h);
      setTax(t);
      setRecs(r);
      setBrokerBreakdown(bb);
    } catch (err) {
      setError((err as Error).message);
      throw err;
    } finally {
      setLoading(false);
    }
  };

  const loadBrokerStatus = async () => {
    try {
      const status = await api.getBrokerStatus();
      setBrokers(status);
      const connected = status.some((b) => b.connected);
      setHasConnectedBroker(connected);
      return connected;
    } catch {
      setBrokers([]);
      setHasConnectedBroker(false);
      return false;
    }
  };

  const handleLogin = async (broker: string) => {
    setBusyBroker(broker);
    setBrokerAction(null);
    try {
      const res = await api.loginBroker(broker);
      setBrokerAction(res.message);
      await loadBrokerStatus();
    } catch (e) {
      setBrokerAction((e as Error).message);
    } finally {
      setBusyBroker(null);
    }
  };

  const handleSync = async (broker: string) => {
    setBusyBroker(broker);
    setBrokerAction(null);
    setError(null);
    try {
      const res = await api.syncBroker(broker);
      setSyncStatus((prev) => ({
        ...prev,
        [broker]: { at: new Date().toISOString(), result: res }
      }));
      setBrokerAction(res.message);
      if (res.success || res.price_refresh_success) {
        await loadData();
        await loadBrokerStatus();
      }
      if (!res.success) {
        setError(res.message);
      }
    } catch (e) {
      const message = (e as Error).message;
      setBrokerAction(message);
      setError(message);
    } finally {
      setBusyBroker(null);
    }
  };

  useEffect(() => {
    (async () => {
      const connected = await loadBrokerStatus();
      if (connected) {
        await loadData();
      } else {
        clearPortfolioData();
        setLoading(false);
      }
    })();
  }, []);

  return (
    <main className="container">
      <header>
        <h1>Cadmium</h1>
        <nav>
          <button onClick={() => setTab("dashboard")}>Dashboard</button>
          <button onClick={() => setTab("portfolio")}>Portfolio</button>
          <button onClick={() => setTab("tax")}>Tax Analysis</button>
        </nav>
      </header>

      <section className="card">
        <h3>Broker Connections</h3>
        <p>Click Login, finish OTP/2FA/captcha in opened browser window (up to 10 minutes), then click Sync.</p>
        <p>Note: Zerodha uses one session and syncs both Kite (stocks/ETFs) and Coin (mutual funds).</p>
        {brokerAction && <p>{brokerAction}</p>}
        <div className="broker-grid">
          {brokers.map((b) => (
            <div key={b.broker} className="broker-row">
              <strong>{b.broker}</strong>
              <span>{b.connected ? "Connected" : "Not connected"}</span>
              <button disabled={busyBroker === b.broker} onClick={() => handleLogin(b.broker)}>
                {busyBroker === b.broker ? "Working..." : "Login"}
              </button>
              <button disabled={busyBroker === b.broker || !b.connected} onClick={() => handleSync(b.broker)}>
                Sync
              </button>
              {syncStatus[b.broker] && (
                <div>
                  <span>
                    Last sync: {syncStatus[b.broker].result.success ? "success" : "failed"} | Data quality:{" "}
                    {syncStatus[b.broker].result.data_quality}
                  </span>
                  <span>
                    {" "}
                    | Lot refresh: {syncStatus[b.broker].result.lot_refresh_success ? "ok" : "failed"} | Price refresh:{" "}
                    {syncStatus[b.broker].result.price_refresh_success ? "ok" : "failed"}
                  </span>
                  {syncStatus[b.broker].result.upstream_error_code ? (
                    <span> | Upstream: {syncStatus[b.broker].result.upstream_error_code}</span>
                  ) : null}
                  <span> | Reason: {syncStatus[b.broker].result.message}</span>
                </div>
              )}
            </div>
          ))}
        </div>
      </section>

      {error && (
        <div className="card">
          <p className="error">{error}</p>
          <button
            onClick={async () => {
              const connected = await loadBrokerStatus();
              if (connected) {
                await loadData();
              } else {
                clearPortfolioData();
              }
            }}
          >
            Retry
          </button>
        </div>
      )}
      {!hasConnectedBroker ? (
        <div className="card">
          <p>No broker connected. Connect at least one broker, then click Sync to load real portfolio data.</p>
        </div>
      ) : loading ? (
        <p>Loading...</p>
      ) : !summary || !tax || !recs ? (
        <p>No data available. Check backend and click Retry.</p>
      ) : (
        <>
          {tab === "dashboard" && (
            <Dashboard summary={summary} tax={tax} brokerBreakdown={brokerBreakdown} />
          )}
          {tab === "portfolio" && <Portfolio holdings={holdings} />}
          {tab === "tax" && <TaxAnalysisPage tax={tax} recs={recs} />}
        </>
      )}
    </main>
  );
}
