from typing import Callable, Tuple
from src.infrastructure.config import TestSettings, settings, Settings
from src.services.parser_service import ParserService
from src.services.db_service import DatabaseService
from src.infrastructure.downloader import download_to_tempfile


# Default implementations of injection
def get_settings() -> Settings:
    """Provide service settings as a dependency."""
    return settings

def get_testing_settings() -> TestSettings:
    """Provide testing settings as a dependency."""
    settings = TestSettings()
    return settings

def get_downloader() -> Callable[[str, int], Tuple[str, int, int]]:
    """Provide file downloader function as a dependency.
    
    Returns a function that takes (url, timeout_seconds) and returns
    (local_path, size_bytes, elapsed_ms).
    """
    return download_to_tempfile

# TODO: Make dependency injection for AMQP consumer

def get_parser_service() -> ParserService:
    """Provide ParserService as a dependency."""
    return ParserService()


def get_db_service() -> DatabaseService:
    """Provide DatabaseService as a dependency."""
    return DatabaseService()
