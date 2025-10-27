from pydantic import BaseSettings

class Settings(BaseSettings):
    api_football_key: str
    oddsapi_key: str
    db_path: str = "warehouse.duckdb"
    leagues: list[str] = ["soccer_epl", "soccer_germany_bundesliga"]

    class Config:
        env_file = ".env"

settings = Settings()
