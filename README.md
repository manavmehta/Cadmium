# Cadmium

Initial scaffold from `cadmium_design_doc.md`.

## Backend (FastAPI)

```bash
cd /Users/manavmehta/repos/Cadmium/backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

API docs: `http://localhost:8000/docs`

## Frontend (React + Vite)

```bash
cd /Users/manavmehta/repos/Cadmium/frontend
npm install
npm run dev
```

App: `http://localhost:5173`

## Implemented now

- Core backend architecture + routers/services/models/schemas
- SQLite DB initialization and local seed data
- Tax analysis and basic LTCG harvesting recommendation logic
- Frontend pages and components wired to backend

## Next build targets

- Playwright broker scrapers for Zerodha/Groww/INDmoney/Coin
- Real realized-gain computation from transaction lots
- AI service integration with LAN-hosted model endpoint
- Auth/session handling for scraper login persistence
