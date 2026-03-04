from pydantic import BaseModel


class Settings(BaseModel):
    app_name: str = "Cadmium API"
    api_prefix: str = "/api"
    database_url: str = "sqlite:///./cadmium.db"
    ai_service_url: str = "http://192.168.1.10:8001/analyze"


settings = Settings()
