from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.database import get_db
from app.schemas.api_schemas import BrokerActionResponse, BrokerStatusItem, BrokerSyncResult
from app.services.broker_service import BrokerService

router = APIRouter(prefix="/brokers", tags=["brokers"])


@router.get("/status", response_model=list[BrokerStatusItem])
async def broker_status():
    return BrokerService.broker_status()


@router.post("/{broker}/login", response_model=BrokerActionResponse)
async def login_broker(broker: str, wait_seconds: int = 600):
    broker = broker.lower()
    try:
        message = await BrokerService.login_broker(broker, wait_seconds=wait_seconds)
        return BrokerActionResponse(broker=broker, success=True, message=message)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/{broker}/sync", response_model=BrokerActionResponse)
async def sync_one_broker(broker: str, db: Session = Depends(get_db)):
    broker = broker.lower()
    try:
        result = await BrokerService.sync_broker_holdings(db, broker)
        return BrokerActionResponse(**result)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/sync", response_model=list[BrokerSyncResult])
async def sync_brokers(db: Session = Depends(get_db)):
    return await BrokerService.sync_all_brokers(db)
