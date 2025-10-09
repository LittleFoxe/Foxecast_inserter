import os
from dataclasses import dataclass
from dotenv import load_dotenv


load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))


@dataclass
class Settings:
    """Service configuration loaded from environment variables."""

    # ClickHouse
    ch_host: str = os.getenv("CH_HOST", "clickhouse")
    ch_port: int = int(os.getenv("CH_PORT", "8123"))
    ch_user: str = os.getenv("CH_USER", "default")
    ch_password: str = os.getenv("CH_PASSWORD", "")
    ch_database: str = os.getenv("CH_DATABASE", "forecast_main")

    # RabbitMQ (WIP, no implementation yet)
    rabbitmq_url: str = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
    rabbitmq_queue: str = os.getenv("RABBITMQ_QUEUE", "forecast_files")
    rabbitmq_prefetch: int = int(os.getenv("RABBITMQ_PREFETCH", "4"))

    # Download and parsing
    download_timeout_seconds: int = int(os.getenv("DOWNLOAD_TIMEOUT_SECONDS", "300"))

    # Using metrics
    enable_metrics: bool = os.getenv("ENABLE_METRICS", "true").lower() == "true"


settings = Settings()
