from typing import Callable, Tuple

from src.infrastructure.config import Settings, TestSettings, settings, test_settings
from src.services.parser_service import ParserService
from src.services.db_service import DatabaseService
from src.infrastructure.downloader import download_to_tempfile


# Default implementations of injection
def get_settings() -> Settings:
    """Provide service settings as a dependency."""
    return settings

def get_testing_settings() -> TestSettings:
    """Provide settings for testing as a dependency."""
    return test_settings

def get_downloader() -> Callable[[str, int], Tuple[str, int, int]]:
    """Provide file downloader function as a dependency.
    
    Returns a function that takes (url, timeout_seconds) and returns
    (local_path, size_bytes, elapsed_ms).
    """
    return download_to_tempfile

# TODO: Make dependency injection for AMQP consumer

def get_parser_service() -> ParserService:
    """Provide parser service as a dependency."""
    return ParserService()

def get_db_service() -> DatabaseService:
    """
    Provide database service as a dependency.
    
    Args:
        settings (Settings): Configuration settings with env variables
    """
    return DatabaseService(get_settings())
