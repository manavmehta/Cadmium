from app.schemas.api_schemas import AIRequest, AIResponse


class AIService:
    @staticmethod
    async def analyze(payload: AIRequest) -> AIResponse:
        return AIResponse(
            stocks_to_sell=[],
            sell_quantities=[],
            expected_gain=0.0,
            reasoning=(
                "AI integration is not configured yet. "
                "Configure local model endpoint before using /api/ai/analyze."
            ),
        )
