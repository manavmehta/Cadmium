from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.holding import Holding
from app.models.transaction import Transaction
from app.schemas.api_schemas import HarvestRecommendationOut, TaxAnalysisOut
from app.services.tax_service import TaxService

router = APIRouter(prefix="/tax", tags=["tax"])


@router.get("/analysis", response_model=TaxAnalysisOut)
def get_tax_analysis(db: Session = Depends(get_db)):
    holdings = db.query(Holding).all()
    transactions = db.query(Transaction).all()
    return TaxService.analyze(holdings, transactions)


@router.get("/harvest", response_model=HarvestRecommendationOut)
def get_harvest_recommendations(db: Session = Depends(get_db)):
    holdings = db.query(Holding).all()
    tax = TaxService.analyze(holdings, [])
    return TaxService.recommend_harvest(holdings, tax.remaining_tax_free_ltcg)
