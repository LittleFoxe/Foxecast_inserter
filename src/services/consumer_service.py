from collections.abc import Callable

from src.metrics.metrics import update_all_metrics
from src.services.db_service import DatabaseService
from src.services.parser_service import ParserService


class BrokerServicesDTO:
    """The contract to serve necessary services through common app infrastructure.
    """

    def __init__(
            self,
            downloader: Callable[[str, int], tuple[str, int, int]],
            parser: ParserService,
            db: DatabaseService) -> None:
        self.downloader = downloader
        self.parser = parser
        self.db = db

def handle_message(url: str, services: BrokerServicesDTO, download_timeout_seconds: int = 300) -> None:
    """Handle the message from the broker independently of the technology used.
    Should be used inside try/except statement to process the internal issues.

    Args:
        url (str): Location of the file to parse and upload its data to the database
        download_timeout_seconds (int): Max amount of time in seconds to download the file from the URL

    """
    # Parsing the file's name from its URL
    file_name = url.split("/")[-1]

    # Initializing services from the provider
    downloader = services.downloader
    parser = services.parser
    db = services.db

    # Inserting the data to the database
    local_path, size_bytes, download_ms = downloader(url, download_timeout_seconds)
    dtos, parse_ms = parser.parse_file(local_path, file_name=file_name)
    _, db_ms = db.insert_batch(dtos, file_name=file_name)

    # Updating metrics
    update_all_metrics(
        download_ms=download_ms,
        parse_ms=parse_ms,
        db_ms=db_ms,
        file_size=size_bytes)
