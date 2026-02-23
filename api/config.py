"""
Application settings from environment variables.
"""
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Database
    flights_db_host: str = "localhost"
    flights_db_port: int = 5432
    flights_db_name: str = "flights"
    flights_db_user: str = "flights_user"
    flights_db_password: str = "flights_pass"

    # Redis
    redis_host: str = "localhost"
    redis_port: int = 6379

    # JWT
    jwt_secret: str = "change-me-in-production"
    jwt_algorithm: str = "HS256"
    jwt_expire_minutes: int = 60

    # API keys (pre-configured for demo)
    api_key: str = "flight-pipeline-demo-key-2025"
    admin_api_key: str = "flight-pipeline-admin-key-2025"

    # Read replica (empty = use primary)
    flights_db_replica_host: str = ""

    # Pool
    db_pool_min: int = 2
    db_pool_max: int = 10

    # Rate limiting
    rate_limit_per_minute: int = 100

    # LLM
    anthropic_api_key: str = ""


settings = Settings()
