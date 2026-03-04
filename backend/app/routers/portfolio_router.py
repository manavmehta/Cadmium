from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.holding import Holding
from app.schemas.api_schemas import HoldingOut, PortfolioBrokerBreakdown, PortfolioSummaryOut
from app.services.portfolio_service import PortfolioService

router = APIRouter(prefix="/portfolio", tags=["portfolio"])


@router.get("/holdings", response_model=list[HoldingOut])
def get_holdings(db: Session = Depends(get_db)):
    holdings = db.query(Holding).all()
    return PortfolioService.holdings_aggregated(holdings)


@router.get("/summary", response_model=PortfolioSummaryOut)
def get_summary(db: Session = Depends(get_db)):
    holdings = db.query(Holding).all()
    return PortfolioService.summarize(holdings)


@router.get("/broker-breakdown", response_model=list[PortfolioBrokerBreakdown])
def get_broker_breakdown(db: Session = Depends(get_db)):
    holdings = db.query(Holding).all()
    return PortfolioService.broker_breakdown(holdings)
