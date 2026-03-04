import re
import time
import logging
import json
import tempfile
import shutil
import uuid
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import httpx
from sqlalchemy.orm import Session

from app.models.holding import Holding

logger = logging.getLogger(__name__)


class DataQualityError(RuntimeError):
    pass


@dataclass(frozen=True)
class BrokerConfig:
    name: str
    login_url: str
    holdings_sources: list[tuple[str, str]]
    login_done_url_markers: list[str]
    auth_cookie_names: list[str]
    auth_cookie_hints: list[str]
    require_url_marker_for_success: bool = False


BROKER_CONFIG: dict[str, BrokerConfig] = {
    "zerodha": BrokerConfig(
        name="zerodha",
        login_url="https://kite.zerodha.com/",
        holdings_sources=[
            ("kite", "https://kite.zerodha.com/holdings"),
            ("coin", "https://coin.zerodha.com/dashboard/holdings"),
        ],
        login_done_url_markers=[
            "kite.zerodha.com/holdings",
            "kite.zerodha.com/dashboard",
            "coin.zerodha.com/dashboard",
        ],
        auth_cookie_names=["enctoken"],
        auth_cookie_hints=[],
    ),
    "groww": BrokerConfig(
        name="groww",
        login_url="https://groww.in/login",
        holdings_sources=[("default", "https://groww.in/user/holdings")],
        login_done_url_markers=[
            "groww.in/user/",
            "groww.in/stocks/",
        ],
        auth_cookie_names=[],
        auth_cookie_hints=["access_token", "id_token", "refresh_token", "groww"],
        require_url_marker_for_success=True,
    ),
    "indmoney": BrokerConfig(
        name="indmoney",
        login_url="https://indmoney.com/login",
        holdings_sources=[("default", "https://indmoney.com/stocks/holdings")],
        login_done_url_markers=[
            "indmoney.com/stocks/",
            "indmoney.com/dashboard",
            "indmoney.com/account",
        ],
        auth_cookie_names=[],
        auth_cookie_hints=["access_token", "id_token", "refresh_token", "auth_token", "jwt"],
        require_url_marker_for_success=True,
    ),
}


class BrokerService:
    SESSIONS_DIR = Path(__file__).resolve().parents[2] / "sessions"
    LOT_EPSILON = 1e-6
    LOT_SYNC_V2 = os.getenv("LOT_SYNC_V2", "1").lower() in {"1", "true", "yes", "on"}
    ZERODHA_CONSOLE_ORIGIN = "https://console.zerodha.com"

    @classmethod
    def _session_path(cls, broker: str) -> Path:
        return cls.SESSIONS_DIR / f"{broker}.json"

    @staticmethod
    def supported_brokers() -> list[str]:
        return sorted(BROKER_CONFIG.keys())

    @classmethod
    def broker_status(cls) -> list[dict]:
        cls.SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
        out: list[dict] = []
        for broker in cls.supported_brokers():
            session_file = cls._session_path(broker)
            out.append(
                {
                    "broker": broker,
                    "connected": cls._is_session_authenticated(broker, session_file),
                    "session_file": str(session_file),
                }
            )
        return out

    @classmethod
    def _is_session_authenticated(cls, broker: str, session_file: Path) -> bool:
        if not session_file.exists():
            return False
        try:
            raw = json.loads(session_file.read_text())
            cookies = raw.get("cookies", [])
            config = BROKER_CONFIG[broker]
            named_ok = any(cls._has_cookie(cookies, name) for name in config.auth_cookie_names)
            hinted_ok = cls._has_any_cookie_hint(cookies, config.auth_cookie_hints)
            return named_ok or hinted_ok
        except Exception:
            return False

    @staticmethod
    def _load_session_cookies(session_file: Path) -> list[dict]:
        raw = json.loads(session_file.read_text())
        return raw.get("cookies", [])

    @classmethod
    def _extract_cookie_value(cls, session_file: Path, cookie_name: str) -> str | None:
        try:
            cookies = cls._load_session_cookies(session_file)
        except Exception:
            return None
        for cookie in cookies:
            if str(cookie.get("name", "")).lower() == cookie_name.lower():
                value = str(cookie.get("value", "")).strip()
                if value:
                    return value
        return None

    @staticmethod
    def _looks_logged_in(url: str, markers: list[str]) -> bool:
        lower = (url or "").lower()
        return any(marker in lower for marker in markers)

    @staticmethod
    def _url_auth_heuristic(broker: str, url: str) -> bool:
        u = (url or "").lower()
        if not u:
            return False
        if broker == "zerodha":
            # Zerodha has reliable cookie + URL markers, so avoid heuristic shortcuts.
            return False
        if broker == "groww":
            return (
                "groww.in" in u
                and "/login" not in u
                and "captcha" not in u
                and "otp" not in u
            )
        if broker == "indmoney":
            return (
                "indmoney.com" in u
                and "/login" not in u
                and "captcha" not in u
                and "otp" not in u
                and "verify" not in u
            )
        return False

    @staticmethod
    def _has_cookie(cookies: list[dict], cookie_name: str) -> bool:
        for cookie in cookies:
            name = str(cookie.get("name", "")).lower()
            value = str(cookie.get("value", "")).strip()
            if name == cookie_name.lower() and value:
                return True
        return False

    @staticmethod
    def _has_any_cookie_hint(cookies: list[dict], hints: list[str]) -> bool:
        deny_exact = {
            "user_session_id",
            "_grecaptcha",
            "__cf_bm",
            "_ga",
            "_gid",
        }
        deny_prefixes = ("_ga_", "mp_", "ajs_")
        for cookie in cookies:
            name = str(cookie.get("name", "")).lower()
            value = str(cookie.get("value", "")).strip()
            if not value:
                continue
            if name in deny_exact or any(name.startswith(prefix) for prefix in deny_prefixes):
                continue
            if len(value) < 16:
                continue
            if any(h in name for h in hints):
                return True
        return False

    @classmethod
    async def _has_authenticated_session(cls, context, broker: str) -> bool:
        cookies = await context.cookies()
        config = BROKER_CONFIG[broker]
        named_ok = any(cls._has_cookie(cookies, name) for name in config.auth_cookie_names)
        hinted_ok = cls._has_any_cookie_hint(cookies, config.auth_cookie_hints)
        return named_ok or hinted_ok

    @staticmethod
    async def _open_login_context(playwright):
        launch_args = [
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-gpu",
        ]
        last_error: Exception | None = None

        async def launch_context(channel: str | None):
            browser = await playwright.chromium.launch(
                headless=False,
                channel=channel,
                args=launch_args,
            )
            context = await browser.new_context(viewport={"width": 1440, "height": 900})
            return context, None, browser

        # Use Playwright Chromium first for stability, then fallback to local Chrome.
        for channel in (None, "chrome"):
            try:
                return await launch_context(channel)
            except Exception as exc:
                last_error = exc
                logger.warning("Login launch failed for channel=%s: %s", channel or "playwright-chromium", exc)

        raise RuntimeError(f"Unable to launch login browser: {last_error}")

    @classmethod
    async def login_broker(cls, broker: str, wait_seconds: int = 180) -> str:
        from playwright.async_api import async_playwright

        config = BROKER_CONFIG.get(broker)
        if not config:
            raise ValueError(f"Unsupported broker '{broker}'.")

        cls.SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
        session_path = cls._session_path(broker)
        prior_mtime = session_path.stat().st_mtime if session_path.exists() else 0.0

        try:
            logger.warning("Starting broker login for %s with timeout=%ss", broker, wait_seconds)
            async with async_playwright() as p:
                context, profile_dir, fallback_browser = await cls._open_login_context(p)
                try:
                    page = context.pages[0] if context.pages else await context.new_page()
                    await page.goto(config.login_url, wait_until="domcontentloaded")

                    started = time.monotonic()
                    confirmed_since: float | None = None
                    stable_confirm_seconds = 2.0
                    while (time.monotonic() - started) < wait_seconds:
                        live_pages = [pg for pg in context.pages if not pg.is_closed()]
                        if not live_pages:
                            raise RuntimeError(
                                f"Login window for '{broker}' was closed before authentication completed."
                            )
                        active_page = live_pages[-1]

                        cookie_ok = await cls._has_authenticated_session(context, broker)
                        marker_ok = any(cls._looks_logged_in(pg.url, config.login_done_url_markers) for pg in live_pages)
                        heuristic_ok = any(cls._url_auth_heuristic(broker, pg.url) for pg in live_pages)

                        if broker == "zerodha":
                            # Strict mode: avoid false success on initial navigation.
                            signal_ok = cookie_ok and marker_ok
                        elif config.require_url_marker_for_success:
                            signal_ok = marker_ok and (cookie_ok or heuristic_ok)
                        else:
                            signal_ok = cookie_ok or marker_ok or heuristic_ok
                        if signal_ok:
                            if confirmed_since is None:
                                confirmed_since = time.monotonic()
                            if (time.monotonic() - confirmed_since) >= stable_confirm_seconds:
                                if broker == "zerodha":
                                    console_ok = await cls._zerodha_console_is_authenticated(context)
                                    if not console_ok:
                                        confirmed_since = None
                                        await active_page.wait_for_timeout(800)
                                        continue
                                await context.storage_state(path=str(session_path))
                                updated_session = session_path.exists() and session_path.stat().st_mtime > prior_mtime
                                if not updated_session:
                                    raise RuntimeError(
                                        f"Login for '{broker}' was detected but session file was not refreshed."
                                    )
                                logger.warning("Broker login completed for %s", broker)
                                return (
                                    f"Login completed for '{broker}'. "
                                    f"Session saved to {session_path}."
                                )
                        else:
                            confirmed_since = None

                        await active_page.wait_for_timeout(500)

                    raise RuntimeError(
                        f"Login timeout for '{broker}' after {wait_seconds} seconds. "
                        "Complete login and retry."
                    )
                finally:
                    try:
                        await context.close()
                    except Exception:
                        pass
                    if fallback_browser is not None:
                        try:
                            await fallback_browser.close()
                        except Exception:
                            pass
                    if profile_dir is not None:
                        shutil.rmtree(profile_dir, ignore_errors=True)
        except Exception as exc:
            msg = str(exc)
            if "Executable doesn't exist" in msg or "Please run the following command" in msg:
                raise RuntimeError(
                    "Playwright Chromium is not installed. Run: "
                    "'cd /Users/manavmehta/repos/Cadmium/backend && "
                    "source .venv/bin/activate && playwright install chromium'"
                ) from exc
            logger.exception("Broker login failed for %s", broker)
            raise

    @staticmethod
    def _infer_asset_type(row: list[str], source: str) -> str:
        if source == "coin":
            return "mf"
        text = " ".join(row).lower()
        if "mutual fund" in text or " fund " in f" {text} ":
            return "mf"
        if "etf" in text:
            return "etf"
        return "stock"

    @staticmethod
    def _parse_number(raw: str) -> float | None:
        cleaned = raw.replace(",", "").replace("₹", "").strip()
        match = re.search(r"-?\d+(\.\d+)?", cleaned)
        if not match:
            return None
        return float(match.group(0))

    @staticmethod
    def _safe_float(value) -> float:
        if value is None:
            return 0.0
        if isinstance(value, (int, float)):
            return float(value)
        parsed = BrokerService._parse_number(str(value))
        return float(parsed) if parsed is not None else 0.0

    @staticmethod
    def _parse_date_string(raw: str) -> date | None:
        value = (raw or "").strip()
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
        except ValueError:
            pass
        formats = [
            "%Y-%m-%d",
            "%Y-%m-%d %H:%M:%S",
            "%d-%m-%Y",
            "%d/%m/%Y",
            "%d %b %Y",
            "%d %B %Y",
        ]
        for fmt in formats:
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                continue
        return None

    @classmethod
    def _sync_failure(cls, broker: str, message: str, error_code: str = "DATA_QUALITY") -> dict:
        return {
            "broker": broker,
            "success": False,
            "message": message,
            "holdings_synced": 0,
            "lots_synced": 0,
            "data_quality": "unreliable",
            "error_code": error_code,
        }

    @classmethod
    def _lot_key(cls, holding: Holding) -> tuple[str, str, str]:
        return (
            (holding.symbol or "").upper(),
            holding.isin or "",
            (holding.asset_type or "stock").lower(),
        )

    @classmethod
    def _aggregate_snapshot_holdings(cls, holdings: list[Holding]) -> list[Holding]:
        by_key: dict[tuple[str, str, str], Holding] = {}
        for holding in holdings:
            key = cls._lot_key(holding)
            existing = by_key.get(key)
            if not existing:
                by_key[key] = holding
                continue

            new_total = existing.quantity + holding.quantity
            if new_total > cls.LOT_EPSILON:
                existing.average_buy_price = (
                    (existing.average_buy_price * existing.quantity)
                    + (holding.average_buy_price * holding.quantity)
                ) / new_total
            existing.quantity = new_total
            existing.current_price = holding.current_price
            if holding.buy_date < existing.buy_date:
                existing.buy_date = holding.buy_date
        return list(by_key.values())

    @classmethod
    def _apply_snapshot_to_lots(cls, db: Session, broker: str, snapshot_holdings: list[Holding]) -> int:
        snapshots = cls._aggregate_snapshot_holdings(snapshot_holdings)
        if not snapshots:
            raise RuntimeError(f"No holdings snapshot received for {broker}.")

        # Guardrail: avoid committing misleading LT/ST if broker snapshot lacks acquisition dates.
        # If all lots are stamped as today on first/empty import, classification would be wrong.
        existing_count = db.query(Holding).filter(Holding.broker == broker).count()
        if existing_count == 0 and all(h.buy_date == date.today() for h in snapshots):
            raise RuntimeError(
                f"{broker} sync did not provide acquisition dates for holdings. "
                "LT/ST classification is unavailable until lot-level trade history is synced."
            )
        recent_cutoff = date.today().toordinal() - 2
        recent_like = sum(1 for h in snapshots if h.buy_date.toordinal() >= recent_cutoff)
        if len(snapshots) >= 5 and (recent_like / len(snapshots)) >= 0.8:
            raise RuntimeError(
                f"{broker} returned mostly recent acquisition dates ({recent_like}/{len(snapshots)}). "
                "Data quality is insufficient for reliable LT/ST split; sync aborted."
            )

        existing_lots = db.query(Holding).filter(Holding.broker == broker).all()

        existing_by_key: dict[tuple[str, str, str], list[Holding]] = {}
        for lot in existing_lots:
            existing_by_key.setdefault(cls._lot_key(lot), []).append(lot)
        for lots in existing_by_key.values():
            lots.sort(key=lambda h: h.buy_date)

        snapshot_by_key: dict[tuple[str, str, str], Holding] = {
            cls._lot_key(h): h for h in snapshots
        }

        # Remove symbols no longer present.
        for key, lots in existing_by_key.items():
            if key in snapshot_by_key:
                continue
            for lot in lots:
                db.delete(lot)

        for key, snap in snapshot_by_key.items():
            lots = existing_by_key.get(key, [])
            current_qty = sum(max(0.0, lot.quantity) for lot in lots)
            target_qty = max(0.0, snap.quantity)

            if target_qty + cls.LOT_EPSILON < current_qty:
                # Quantity dropped: consume oldest lots first (FIFO assumption).
                to_remove = current_qty - target_qty
                for lot in lots:
                    if to_remove <= cls.LOT_EPSILON:
                        break
                    cut = min(lot.quantity, to_remove)
                    lot.quantity -= cut
                    to_remove -= cut

                for lot in lots:
                    if lot.quantity <= cls.LOT_EPSILON:
                        db.delete(lot)

            elif target_qty > current_qty + cls.LOT_EPSILON:
                # Quantity increased: add a new lot for delta at current snapshot date.
                delta = target_qty - current_qty
                db.add(
                    Holding(
                        symbol=snap.symbol,
                        isin=snap.isin,
                        broker=broker,
                        quantity=delta,
                        average_buy_price=snap.average_buy_price,
                        buy_date=snap.buy_date,
                        current_price=snap.current_price,
                        asset_type=snap.asset_type,
                    )
                )

            # Refresh market price and metadata for remaining lots.
            remaining = db.query(Holding).filter(
                Holding.broker == broker,
                Holding.symbol == snap.symbol,
                Holding.isin == snap.isin,
                Holding.asset_type == snap.asset_type,
            ).all()
            for lot in remaining:
                lot.current_price = snap.current_price
                if snap.isin:
                    lot.isin = snap.isin

        db.commit()
        return db.query(Holding).filter(Holding.broker == broker).count()

    @classmethod
    async def _extract_rows_from_page(cls, page, broker: str, source: str) -> list[list[str]]:
        if broker == "groww":
            selectors = [
                "table tbody tr",
                "[data-testid='holdings-table'] tbody tr",
                "[class*='holding'] table tbody tr",
            ]
        elif broker == "zerodha":
            selectors = [
                "table tbody tr",
                ".holdings-table tbody tr",
            ]
        elif broker == "indmoney":
            selectors = [
                "table tbody tr",
                "[class*='portfolio'] table tbody tr",
                "[class*='holding'] table tbody tr",
            ]
        else:
            selectors = ["table tbody tr"]

        rows = await page.evaluate(
            """(sels) => {
                let rowNodes = [];
                for (const sel of sels) {
                    rowNodes = Array.from(document.querySelectorAll(sel));
                    if (rowNodes.length > 0) break;
                }
                const tableRows = rowNodes.map((row) =>
                    Array.from(row.querySelectorAll('td')).map((td) =>
                        (td.innerText || '').replace(/\\s+/g, ' ').trim()
                    )
                ).filter((r) => r.length > 0);
                if (tableRows.length > 0) return tableRows;

                // Fallback for card-based layouts
                const cardCandidates = Array.from(
                    document.querySelectorAll("[class*='holding'],[class*='portfolio'],[data-testid*='holding']")
                );
                const cardRows = [];
                for (const card of cardCandidates) {
                    const text = (card.innerText || '').replace(/\\s+/g, ' ').trim();
                    if (!text) continue;
                    const hasMoney = /₹|INR|\\d/.test(text);
                    if (!hasMoney) continue;
                    const lines = text.split(/\\s{2,}|\\n/).map((s) => s.trim()).filter(Boolean);
                    if (lines.length >= 3) cardRows.push(lines);
                    if (cardRows.length >= 400) break;
                }
                return cardRows;
            }""",
            selectors,
        )
        return rows

    @classmethod
    async def _sync_zerodha_via_api(cls) -> list[Holding]:
        session_path = cls._session_path("zerodha")
        if not session_path.exists():
            raise RuntimeError("No saved login session for 'zerodha'. Run login first.")

        enctoken = cls._extract_cookie_value(session_path, "enctoken")
        if not enctoken:
            raise RuntimeError("Zerodha session missing enctoken cookie. Please login again.")

        headers_auth = {"Authorization": f"enctoken {enctoken}", "X-Kite-Version": "3"}
        headers_cookie = {"Cookie": f"enctoken={enctoken}", "X-Kite-Version": "3"}
        holdings: list[Holding] = []

        async with httpx.AsyncClient(timeout=30.0) as client:
            async def fetch_json(url: str) -> dict:
                res = await client.get(url, headers=headers_auth)
                if res.status_code in (401, 403):
                    res = await client.get(url, headers=headers_cookie)
                if res.status_code >= 400:
                    raise RuntimeError(f"Zerodha API {url} failed with status {res.status_code}")
                return res.json()

            equity = await fetch_json("https://kite.zerodha.com/oms/portfolio/holdings")
            mf = await fetch_json("https://kite.zerodha.com/oms/mf/holdings")

        for item in equity.get("data", []) or []:
            symbol = str(item.get("tradingsymbol") or "").strip().upper()
            qty = float(item.get("quantity") or 0)
            current = float(item.get("last_price") or 0)
            avg = float(item.get("average_price") or current or 0)
            lot_date = cls._parse_date_string(str(item.get("authorised_date") or "")) or date.today()
            if not symbol or qty <= 0 or current <= 0:
                continue
            holdings.append(
                Holding(
                    symbol=symbol,
                    isin=str(item.get("isin") or ""),
                    broker="zerodha",
                    quantity=qty,
                    average_buy_price=avg,
                    buy_date=lot_date,
                    current_price=current,
                    asset_type="stock" if "etf" not in symbol.lower() else "etf",
                )
            )

        mf_data = mf.get("data", []) or []
        if isinstance(mf_data, dict):
            mf_data = mf_data.get("holdings", []) or []

        for item in mf_data:
            symbol = str(
                item.get("tradingsymbol")
                or item.get("fund")
                or item.get("scheme_name")
                or item.get("isin")
                or ""
            ).strip().upper()
            units = float(item.get("quantity") or item.get("units") or 0)
            current = float(item.get("last_price") or item.get("last_nav") or item.get("nav") or 0)
            avg = float(item.get("average_price") or item.get("avg_nav") or current or 0)
            if not symbol or units <= 0 or current <= 0:
                continue
            holdings.append(
                Holding(
                    symbol=symbol,
                    isin=str(item.get("isin") or ""),
                    broker="zerodha",
                    quantity=units,
                    average_buy_price=avg,
                    buy_date=date.today(),
                    current_price=current,
                    asset_type="mf",
                )
            )

        if not holdings:
            raise RuntimeError("Zerodha API returned no parsable holdings.")
        return holdings

    @classmethod
    async def _console_fetch_json(cls, page, url: str, csrf_token: str) -> dict:
        full_url = url if url.startswith("http") else f"{cls.ZERODHA_CONSOLE_ORIGIN}{url}"
        response = await page.evaluate(
            """async ({u,t}) => {
                try {
                    const r = await fetch(u, {credentials:'include', headers:{'x-csrftoken': t}});
                    const ct = r.headers.get('content-type') || '';
                    return {ok:true, status:r.status, content_type:ct, text: await r.text()};
                } catch (e) {
                    return {ok:false, status:0, content_type:'', text:String(e)};
                }
            }""",
            {"u": full_url, "t": csrf_token},
        )
        if not response.get("ok", True):
            raise RuntimeError(f"Console API request failed for {full_url}: {response.get('text', '')[:180]}")
        if int(response["status"]) >= 400:
            raise RuntimeError(f"Console API error {response['status']} for {full_url}: {response['text'][:180]}")
        ct = str(response.get("content_type", "")).lower()
        if "application/json" not in ct:
            raise RuntimeError(
                f"Console API returned non-JSON response for {full_url} "
                f"(content-type={response.get('content_type', '')})."
            )
        return json.loads(response["text"])

    @classmethod
    async def _console_try_fetch_json(cls, page, url: str, csrf_token: str) -> tuple[int, dict | list | None]:
        full_url = url if url.startswith("http") else f"{cls.ZERODHA_CONSOLE_ORIGIN}{url}"
        response = await page.evaluate(
            """async ({u,t}) => {
                try {
                    const r = await fetch(u, {credentials:'include', headers:{'x-csrftoken': t}});
                    const ct = r.headers.get('content-type') || '';
                    return {ok:true, status:r.status, content_type:ct, text: await r.text()};
                } catch (e) {
                    return {ok:false, status:0, content_type:'', text:String(e)};
                }
            }""",
            {"u": full_url, "t": csrf_token},
        )
        if not response.get("ok", True):
            return 0, None
        status = int(response["status"])
        ct = str(response.get("content_type", "")).lower()
        if "application/json" not in ct:
            return status, None
        text = response.get("text") or ""
        try:
            parsed = json.loads(text)
        except Exception:
            parsed = None
        return status, parsed

    @classmethod
    async def _zerodha_console_is_authenticated(cls, context) -> bool:
        page = context.pages[-1] if context.pages else await context.new_page()
        try:
            await page.goto(f"{cls.ZERODHA_CONSOLE_ORIGIN}/", wait_until="domcontentloaded", timeout=90000)
        except Exception:
            return False
        await page.wait_for_timeout(800)

        if "console.zerodha.com" not in page.url:
            return False

        body_text = (await page.inner_text("body")).lower()
        if "login with kite" in body_text:
            # Attempt SSO bootstrap from existing Kite session.
            try:
                btn = page.get_by_text("LOGIN WITH KITE", exact=False)
                if await btn.count() > 0:
                    await btn.first.click(timeout=2500)
                    await page.wait_for_timeout(2500)
            except Exception:
                pass
            if "console.zerodha.com" not in page.url:
                return False
            body_text = (await page.inner_text("body")).lower()
            if "login with kite" in body_text:
                return False

        cookies = await context.cookies("https://console.zerodha.com")
        token = ""
        for c in cookies:
            if c.get("name") == "public_token":
                token = str(c.get("value") or "")
                break
        if not token:
            return False

        probe_date = (date.today() - timedelta(days=1)).isoformat()
        status, payload = await cls._console_try_fetch_json(
            page, f"/api/reports/holdings/portfolio?date={probe_date}", token
        )
        if status <= 0:
            return False
        if isinstance(payload, dict) and str(payload.get("error_type", "")).lower().endswith("exception"):
            return False
        return status < 400

    @staticmethod
    def _unwrap_rows(payload: dict | list | None) -> list[dict]:
        if payload is None:
            return []
        if isinstance(payload, list):
            return [r for r in payload if isinstance(r, dict)]
        if not isinstance(payload, dict):
            return []
        data = payload.get("data")
        if isinstance(data, list):
            return [r for r in data if isinstance(r, dict)]
        if isinstance(data, dict):
            result = data.get("result")
            if isinstance(result, list):
                return [r for r in result if isinstance(r, dict)]
            holdings = data.get("holdings")
            if isinstance(holdings, list):
                return [r for r in holdings if isinstance(r, dict)]
        result = payload.get("result")
        if isinstance(result, list):
            return [r for r in result if isinstance(r, dict)]
        return []

    @staticmethod
    def _pick_value(row: dict, keys: list[str]):
        for key in keys:
            if key in row and row[key] not in (None, ""):
                return row[key]
        return None

    @classmethod
    async def _console_fetch_holdings_universe(cls, page, csrf_token: str) -> tuple[list[dict], list[dict]]:
        today = date.today().isoformat()
        for i in range(20):
            status, payload = await cls._console_try_fetch_json(
                page, f"/api/reports/holdings/portfolio?date={today}", csrf_token
            )
            if status >= 400:
                raise DataQualityError(
                    f"Console holdings universe API failed with status {status} for date={today}."
                )
            if not isinstance(payload, dict):
                await page.wait_for_timeout(600)
                continue
            data = payload.get("data") or {}
            state = str(data.get("state") or "").upper()
            result = data.get("result") or {}
            if state == "SUCCESS" and isinstance(result, dict):
                eq_rows = result.get("eq") or []
                mf_rows = result.get("mf") or []
                if not isinstance(eq_rows, list):
                    eq_rows = []
                if not isinstance(mf_rows, list):
                    mf_rows = []
                return eq_rows, mf_rows
            if state in {"PENDING", "PROCESSING"}:
                await page.wait_for_timeout(900 + min(i, 8) * 150)
                continue
            await page.wait_for_timeout(600)
        raise DataQualityError("Console holdings universe timed out before SUCCESS.")

    @classmethod
    async def _console_fetch_holdings_breakdown_rows(
        cls, page, csrf_token: str, instrument_id: str, segment: str
    ) -> list[dict]:
        url = f"/api/reports/holdings/breakdown?instrument_id={instrument_id}&segment={segment}"
        payload = await cls._console_fetch_json(page, url, csrf_token)
        return cls._unwrap_rows(payload)

    @classmethod
    def _build_zerodha_snapshot_maps(
        cls, snapshot: list[Holding]
    ) -> tuple[dict[tuple[str, str], tuple[float, str, str]], dict[tuple[str, str], float], float]:
        price_map: dict[tuple[str, str], tuple[float, str, str]] = {}
        snapshot_qty: dict[tuple[str, str], float] = {}
        snapshot_mf_total_qty = 0.0
        for h in snapshot:
            symbol = (h.symbol or "").upper()
            isin = (h.isin or "").upper()
            key_exact = (symbol, isin)
            key_symbol = (symbol, "")
            key_isin = ("", isin) if isin else None
            for key in [key_exact, key_symbol] + ([key_isin] if key_isin else []):
                if key is None:
                    continue
                price_map[key] = (h.current_price, h.isin, h.asset_type)
                snapshot_qty[key] = snapshot_qty.get(key, 0.0) + h.quantity
            if (h.asset_type or "").lower() == "mf":
                snapshot_mf_total_qty += h.quantity
        return price_map, snapshot_qty, snapshot_mf_total_qty

    @classmethod
    def _resolve_snapshot_meta(
        cls, price_map: dict[tuple[str, str], tuple[float, str, str]], symbol: str, isin: str, segment: str
    ) -> tuple[float, str, str]:
        symbol_u = (symbol or "").upper()
        isin_u = (isin or "").upper()
        candidates = [(symbol_u, isin_u), (symbol_u, ""), ("", isin_u)]
        for key in candidates:
            if key in price_map:
                return price_map[key]
        asset_type = "mf" if segment == "MF" else "stock"
        return 0.0, isin, asset_type

    @classmethod
    def _build_lots_from_breakdown(
        cls,
        instrument_row: dict,
        breakdown_rows: list[dict],
        segment: str,
        price_map: dict[tuple[str, str], tuple[float, str, str]],
        sync_run_id: str,
    ) -> list[Holding]:
        symbol = str(
            cls._pick_value(instrument_row, ["tradingsymbol", "symbol", "ticker", "name", "instrument"])
            or ""
        ).strip().upper()
        isin = str(cls._pick_value(instrument_row, ["isin"]) or "").strip().upper()
        instrument_qty = cls._safe_float(
            cls._pick_value(
                instrument_row,
                ["quantity_available", "total_quantity", "quantity", "qty", "units", "net_quantity"],
            )
        )
        current_price, resolved_isin, asset_type = cls._resolve_snapshot_meta(price_map, symbol, isin, segment)
        if current_price <= 0:
            current_price = cls._safe_float(
                cls._pick_value(instrument_row, ["last_price", "ltp", "nav", "current_price", "market_price"])
            )

        lots: list[Holding] = []
        parsed_qty = 0.0
        for row in breakdown_rows:
            side = str(cls._pick_value(row, ["trade_type", "side", "transaction_type"]) or "buy").lower()
            if side not in {"buy", "b", "purchase"}:
                continue
            qty = cls._safe_float(
                cls._pick_value(row, ["quantity", "qty", "units", "balance_units", "remaining_quantity", "open_quantity"])
            )
            if qty <= cls.LOT_EPSILON:
                continue
            buy_date_raw = cls._pick_value(
                row,
                [
                    "buy_date",
                    "purchase_date",
                    "acquisition_date",
                    "trade_date",
                    "order_execution_time",
                    "allotment_date",
                    "date",
                ],
            )
            buy_date = cls._parse_date_string(str(buy_date_raw or ""))
            if buy_date is None:
                continue
            avg_price = cls._safe_float(
                cls._pick_value(row, ["average_price", "avg_price", "purchase_price", "buy_price", "nav", "price"])
            )
            if avg_price <= 0:
                avg_price = current_price
            lots.append(
                Holding(
                    symbol=symbol,
                    isin=resolved_isin or isin,
                    broker="zerodha",
                    quantity=qty,
                    average_buy_price=avg_price,
                    buy_date=buy_date,
                    current_price=current_price,
                    asset_type=asset_type,
                    lot_source="tradebook",
                    sync_run_id=sync_run_id,
                    data_quality="reliable",
                )
            )
            parsed_qty += qty

        if not lots:
            raise DataQualityError(
                f"No parsable lot rows for {symbol or isin or 'instrument'} in segment {segment}."
            )
        if instrument_qty > cls.LOT_EPSILON and abs(parsed_qty - instrument_qty) > 0.01:
            raise DataQualityError(
                f"Breakdown quantity mismatch for {symbol or isin}: holding={instrument_qty}, lots={round(parsed_qty, 6)}."
            )
        return lots

    @classmethod
    async def _console_fetch_tradebook_rows(cls, page, csrf_token: str, segment: str) -> list[dict]:
        heatmap = await cls._console_fetch_json(page, "/api/reports/tradebook/heatmap", csrf_token)
        segment_map = (((heatmap.get("data") or {}).get("result") or {}).get(segment) or {})
        if not segment_map:
            return []
        dates = sorted(segment_map.keys())
        from_date = dates[0]
        to_date = dates[-1]
        url = f"/api/reports/tradebook?segment={segment}&from_date={from_date}&to_date={to_date}"
        for _ in range(20):
            payload = await cls._console_fetch_json(page, url, csrf_token)
            data = payload.get("data") or {}
            state = data.get("state")
            rows = data.get("result")
            if state == "SUCCESS" and isinstance(rows, list):
                return rows
            await page.wait_for_timeout(1000)
        raise RuntimeError(f"Tradebook generation timed out for segment {segment}.")

    @staticmethod
    def _trade_identity(trade: dict) -> tuple[str, str]:
        symbol = str(
            trade.get("tradingsymbol")
            or trade.get("symbol")
            or trade.get("instrument")
            or trade.get("name")
            or ""
        ).strip().upper()
        isin = str(trade.get("isin") or "").strip().upper()
        return symbol, isin

    @classmethod
    def _build_lots_from_trades(
        cls,
        broker: str,
        trades: list[dict],
        price_map: dict[tuple[str, str], tuple[float, str, str]],
        sync_run_id: str,
    ) -> list[Holding]:
        open_lots: dict[tuple[str, str], list[dict]] = {}

        def resolve_price_key(symbol: str, isin: str) -> tuple[str, str] | None:
            isin_u = isin.upper()
            symbol_u = symbol.upper()
            candidates = [(symbol_u, isin_u), (symbol_u, ""), ("", isin_u)]
            for key in candidates:
                if key in price_map:
                    return key
            return None

        sorted_trades = sorted(
            trades,
            key=lambda t: (
                str(t.get("trade_date") or ""),
                str(t.get("order_execution_time") or ""),
                str(t.get("trade_id") or ""),
            ),
        )
        for trade in sorted_trades:
            symbol, isin = cls._trade_identity(trade)
            if not symbol and not isin:
                continue
            key = (symbol, isin.upper())
            side = str(trade.get("trade_type") or "").lower()
            qty = float(trade.get("quantity") or 0)
            price = float(trade.get("price") or 0)
            tdate = cls._parse_date_string(str(trade.get("trade_date") or "")) or date.today()
            if qty <= 0:
                continue
            lots = open_lots.setdefault(key, [])
            if side == "buy":
                lots.append({"qty": qty, "price": price, "buy_date": tdate})
                continue
            if side == "sell":
                remaining = qty
                while remaining > cls.LOT_EPSILON and lots:
                    cut = min(lots[0]["qty"], remaining)
                    lots[0]["qty"] -= cut
                    remaining -= cut
                    if lots[0]["qty"] <= cls.LOT_EPSILON:
                        lots.pop(0)

        rows: list[Holding] = []
        for key, lots in open_lots.items():
            symbol, isin = key
            resolved_key = resolve_price_key(symbol, isin)
            if resolved_key is None:
                continue
            current_price, snapshot_isin, asset_type = price_map[resolved_key]
            resolved_isin = snapshot_isin or isin
            resolved_symbol = symbol or resolved_key[0]
            for lot in lots:
                if lot["qty"] <= cls.LOT_EPSILON:
                    continue
                rows.append(
                    Holding(
                        symbol=resolved_symbol,
                        isin=resolved_isin,
                        broker=broker,
                        quantity=lot["qty"],
                        average_buy_price=lot["price"] if lot["price"] > 0 else current_price,
                        buy_date=lot["buy_date"],
                        current_price=current_price,
                        asset_type=asset_type,
                        lot_source="tradebook",
                        sync_run_id=sync_run_id,
                        data_quality="reliable",
                    )
                )
        return rows

    @classmethod
    async def _sync_zerodha_lots(cls, db: Session) -> dict:
        if not cls.LOT_SYNC_V2:
            return cls._sync_failure(
                "zerodha",
                "LOT_SYNC_V2 is disabled. Enable LOT_SYNC_V2 to run deterministic lot sync.",
                error_code="FEATURE_FLAG_DISABLED",
            )
        # Snapshot gives current holdings universe + live price.
        snapshot = await cls._sync_zerodha_via_api()
        if not snapshot:
            raise DataQualityError("Zerodha holdings snapshot is empty.")

        price_map, snapshot_qty, snapshot_mf_total_qty = cls._build_zerodha_snapshot_maps(snapshot)
        snapshot_eq_total_qty = sum(
            h.quantity for h in snapshot if (h.asset_type or "").lower() in {"stock", "etf", "equity"}
        )

        from playwright.async_api import async_playwright

        session_path = cls._session_path("zerodha")
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(storage_state=str(session_path))
            console_ok = await cls._zerodha_console_is_authenticated(context)
            if not console_ok:
                await browser.close()
                raise DataQualityError(
                    "Zerodha Console session is not authenticated. "
                    "Run Zerodha login and complete 'LOGIN WITH KITE' on Console."
                )

            cookies = await context.cookies("https://console.zerodha.com")
            token = ""
            for c in cookies:
                if c.get("name") == "public_token":
                    token = str(c.get("value") or "")
                    break
            if not token:
                await browser.close()
                raise DataQualityError("Unable to read Zerodha Console CSRF token.")
            page = context.pages[-1] if context.pages else await context.new_page()

            eq_holdings, mf_holdings = await cls._console_fetch_holdings_universe(page, token)
            await browser.close()

        if snapshot_eq_total_qty > cls.LOT_EPSILON and not eq_holdings:
            raise DataQualityError(
                "Zerodha EQ holdings exist but Console holdings universe (EQ) is unavailable."
            )
        if snapshot_mf_total_qty > cls.LOT_EPSILON and not mf_holdings:
            raise DataQualityError(
                "Zerodha MF holdings exist but Console holdings universe (MF) is unavailable. "
                "Sync aborted due to insufficient lot data quality."
            )

        sync_run_id = uuid.uuid4().hex
        lots: list[Holding] = []
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(storage_state=str(session_path))
            console_ok = await cls._zerodha_console_is_authenticated(context)
            if not console_ok:
                await browser.close()
                raise DataQualityError(
                    "Zerodha Console session is not authenticated for holdings breakdown access."
                )
            cookies = await context.cookies("https://console.zerodha.com")
            token = ""
            for c in cookies:
                if c.get("name") == "public_token":
                    token = str(c.get("value") or "")
                    break
            if not token:
                await browser.close()
                raise DataQualityError("Unable to read Zerodha Console CSRF token for breakdown calls.")
            page = context.pages[-1] if context.pages else await context.new_page()

            for segment, holdings_rows in [("EQ", eq_holdings), ("MF", mf_holdings)]:
                for instrument_row in holdings_rows:
                    instrument_id = str(
                        cls._pick_value(instrument_row, ["instrument_id", "instrument_token", "instrument"])
                        or ""
                    ).strip()
                    if not instrument_id:
                        raise DataQualityError(
                            f"Missing instrument_id for Zerodha {segment} holding row; cannot fetch breakdown."
                        )
                    qty = cls._safe_float(
                        cls._pick_value(
                            instrument_row,
                            ["quantity_available", "total_quantity", "quantity", "qty", "units", "net_quantity"],
                        )
                    )
                    if qty <= cls.LOT_EPSILON:
                        continue
                    breakdown_rows = await cls._console_fetch_holdings_breakdown_rows(
                        page, token, instrument_id, segment
                    )
                    lots.extend(
                        cls._build_lots_from_breakdown(
                            instrument_row, breakdown_rows, segment, price_map, sync_run_id
                        )
                    )
            await browser.close()

        if not lots:
            raise DataQualityError("No open lots could be reconstructed from Zerodha holdings breakdown.")

        snapshot_qty_canonical: dict[tuple[str, str], float] = {}
        for h in snapshot:
            identity = ((h.isin or h.symbol or "").upper(), (h.asset_type or "stock").lower())
            snapshot_qty_canonical[identity] = snapshot_qty_canonical.get(identity, 0.0) + h.quantity

        lot_qty_canonical: dict[tuple[str, str], float] = {}
        for l in lots:
            identity = ((l.isin or l.symbol or "").upper(), (l.asset_type or "stock").lower())
            lot_qty_canonical[identity] = lot_qty_canonical.get(identity, 0.0) + l.quantity

        mismatches: list[str] = []
        for key, snap_q in snapshot_qty_canonical.items():
            lot_q = lot_qty_canonical.get(key, 0.0)
            if abs(snap_q - lot_q) > 0.01:
                label, asset = key
                mismatches.append(f"{label} ({asset}) snapshot={snap_q} lots={lot_q}")
        if mismatches:
            raise DataQualityError(
                "Lot reconstruction mismatch vs Zerodha snapshot: "
                + "; ".join(mismatches[:8])
            )
        try:
            with db.begin():
                db.query(Holding).filter(Holding.broker == "zerodha").delete()
                db.add_all(lots)
        except Exception:
            db.rollback()
            raise
        symbols_count = len({(l.symbol, l.isin) for l in lots})
        return {
            "broker": "zerodha",
            "success": True,
            "message": f"Synced zerodha with lot-level breakdown ({symbols_count} symbols, {len(lots)} lots).",
            "holdings_synced": symbols_count,
            "lots_synced": len(lots),
            "data_quality": "reliable",
            "error_code": None,
        }

    @classmethod
    async def _scrape_rows_for_source(cls, broker: str, source: str, holdings_url: str) -> list[list[str]]:
        from playwright.async_api import async_playwright

        session_path = cls._session_path(broker)
        if not session_path.exists():
            raise ValueError(f"No saved login session for '{broker}'. Run login first.")

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(storage_state=str(session_path))
            page = await context.new_page()
            await page.goto(holdings_url, wait_until="domcontentloaded")
            await page.wait_for_timeout(3500)
            if "login" in page.url.lower():
                await browser.close()
                raise RuntimeError(f"{broker} session expired. Please login again.")

            rows = await cls._extract_rows_from_page(page, broker, source)
            logger.warning("Scrape rows for broker=%s source=%s count=%s url=%s", broker, source, len(rows), page.url)

            await browser.close()
            return rows

    @classmethod
    def _rows_to_holdings(cls, broker: str, rows: list[list[str]], source: str) -> list[Holding]:
        parsed: list[Holding] = []
        for row in rows:
            symbol = row[0].split(" ")[0].upper() if row else ""
            numbers = [n for n in (cls._parse_number(cell) for cell in row) if n is not None]
            if not symbol or len(numbers) < 2:
                continue

            quantity = numbers[0]
            if broker in ("groww", "indmoney"):
                current_price = numbers[1]
                average_buy_price = numbers[2] if len(numbers) > 2 else current_price
            else:
                average_buy_price = numbers[1]
                current_price = numbers[2] if len(numbers) > 2 else numbers[1]
            if quantity <= 0 or current_price <= 0:
                continue

            parsed.append(
                Holding(
                    symbol=symbol,
                    isin="",
                    broker=broker,
                    quantity=quantity,
                    average_buy_price=average_buy_price,
                    buy_date=date.today(),
                    current_price=current_price,
                    asset_type=cls._infer_asset_type(row, source),
                )
            )

            if len(parsed) >= 300:
                break

        return parsed

    @classmethod
    async def sync_broker_holdings(cls, db: Session, broker: str) -> dict:
        config = BROKER_CONFIG.get(broker)
        if not config:
            raise ValueError(f"Unsupported broker '{broker}'.")

        try:
            if broker == "zerodha":
                return await cls._sync_zerodha_lots(db)
            # No-lot policy: if tradebook-level lots are not available, fail explicitly.
            raise DataQualityError(
                f"{broker} lot-level trade history adapter is not implemented yet. "
                "Sync aborted to avoid incorrect LT/ST classification."
            )
        except DataQualityError as exc:
            db.rollback()
            return cls._sync_failure(broker, str(exc))
        except Exception as exc:
            db.rollback()
            logger.exception("Broker sync failed for %s", broker)
            return cls._sync_failure(broker, f"{broker} sync failed: {exc}", error_code="SYNC_FAILED")

    @classmethod
    async def sync_all_brokers(cls, db: Session) -> list[dict]:
        results: list[dict] = []
        for broker in cls.supported_brokers():
            results.append(await cls.sync_broker_holdings(db, broker))
        return results
