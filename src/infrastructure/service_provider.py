from types import CoroutineType
from typing import Awaitable, Callable, Tuple
from aio_pika import IncomingMessage

from src.infrastructure.config import Settings, TestSettings, settings, test_settings
from src.infrastructure.downloader import download_to_tempfile
from src.infrastructure.rabbit_consumer import RabbitHandler
from src.services.parser_service import ParserService
from src.services.db_service import DatabaseService
from src.services.consumer_service import BrokerServicesDTO


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

def get_broker_consumer() -> CoroutineType[None, None, None]:
    """
    Provide an asynchronous consumer to read messages from the queue.
    """
    contract = BrokerServicesDTO(
        downloader=get_downloader(),
        parser=get_parser_service(),
        db=get_db_service()
    )
    
    handler = RabbitHandler(contract, get_settings())
    return handler.run_consumer()

def get_test_message_handler() -> Callable[[IncomingMessage], Awaitable[None]]:
    """
    Provide an additional message handler functions without creating
    the constant connection to the broker.

    Should only be used for basic testing, not for real message processing.

    Returns:
        (Awaitable): function with **aio_pika.IncomingMessage** type
        argument to process the single message
    """
    # Note that in this method get_db_service takes testing env as its main environment
    contract = BrokerServicesDTO(
        downloader=get_downloader(),
        parser=get_parser_service(),
        db=get_db_service()
    )

    handler = RabbitHandler(contract, get_testing_settings())
    return handler._handle_message
