from fastapi import APIRouter

from app.schemas.api_schemas import AIRequest, AIResponse
from app.services.ai_service import AIService

router = APIRouter(prefix="/ai", tags=["ai"])


@router.post("/analyze", response_model=AIResponse)
async def analyze(payload: AIRequest):
    return await AIService.analyze(payload)
