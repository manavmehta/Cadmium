from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text

from app.database import Base, engine
from app.routers.ai_router import router as ai_router
from app.routers.broker_router import router as broker_router
from app.routers.portfolio_router import router as portfolio_router
from app.routers.tax_router import router as tax_router

app = FastAPI(title="Cadmium API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def on_startup() -> None:
    Base.metadata.create_all(bind=engine)
    with engine.begin() as conn:
        cols = {
            row[1] for row in conn.execute(text("PRAGMA table_info(holdings)")).fetchall()
        }
        if "lot_source" not in cols:
            conn.execute(text("ALTER TABLE holdings ADD COLUMN lot_source VARCHAR(32) DEFAULT 'snapshot_derived'"))
        if "sync_run_id" not in cols:
            conn.execute(text("ALTER TABLE holdings ADD COLUMN sync_run_id VARCHAR(64) DEFAULT ''"))
        if "data_quality" not in cols:
            conn.execute(text("ALTER TABLE holdings ADD COLUMN data_quality VARCHAR(16) DEFAULT 'unreliable'"))


@app.get("/health")
def health():
    return {"status": "ok"}


app.include_router(portfolio_router, prefix="/api")
app.include_router(broker_router, prefix="/api")
app.include_router(tax_router, prefix="/api")
app.include_router(ai_router, prefix="/api")
