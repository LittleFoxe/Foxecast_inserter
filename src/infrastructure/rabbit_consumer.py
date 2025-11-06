import asyncio
import json
import logging
from typing import Any, Dict
import aio_pika

from src.metrics.metrics import update_all_metrics
from src.infrastructure.service_provider import get_settings, get_downloader, get_parser_service, get_db_service


logger = logging.getLogger(__name__)
settings = get_settings()


async def handle_message(message: aio_pika.IncomingMessage) -> None:
    """
    General message handler to download the file from message's URL,
    parse it and send to database
    """
    async with message.process(requeue=False):
        body = message.body.decode("utf-8")
        try:
            payload: Dict[str, Any] = json.loads(body)
            url = payload["file"]
        except Exception as exc:
            logger.error("Invalid message: %s - error: %s", body, exc)
            return

        file_name = url.split("/")[-1]

        try:
            # Initializing services from the provider
            downloader = get_downloader()
            parser = get_parser_service()
            db = get_db_service()

            # Inserting 
            local_path, size_bytes, download_ms = downloader(url, settings.download_timeout_seconds)
            dtos, parse_ms = parser.parse_file(local_path, file_name=file_name)
            _, db_ms = db.insert_batch(dtos, file_name=file_name)

            # Updating metrics
            update_all_metrics(
                download_ms=download_ms,
                parse_ms=parse_ms,
                db_ms=db_ms,
                file_size=size_bytes)
        except Exception as exc:
            logger.exception("Failed processing file %s: %s", url, exc)
            # Do not requeue to avoid hot-loop; DLQ should be configured at broker level


async def run_consumer() -> None:
    """Run RabbitMQ consumer in an endless loop with reconnect/backoff on errors.

    All unexpected errors are logged with ERROR level. Shutdown via task cancel
    is handled gracefully.
    """
    backoff_seconds = 1
    connection = None
    while True:
        try:
            connection = await aio_pika.connect_robust(settings.rabbitmq_url)
            logger.info(
                "RabbitMQ connected: url=%s, queue=%s, prefetch=%s",
                settings.rabbitmq_url,
                settings.rabbitmq_queue,
                settings.rabbitmq_prefetch,
            )

            channel = await connection.channel()
            await channel.set_qos(prefetch_count=settings.rabbitmq_prefetch)
            queue = await channel.declare_queue(settings.rabbitmq_queue, durable=True)
            await queue.consume(handle_message)

            # Reset backoff after successful (re)connect
            backoff_seconds = 1

            try:
                await asyncio.Future()  # run forever until cancelled
            finally:
                try:
                    await connection.close()
                except Exception as exc:
                    logger.error("Error while closing RabbitMQ connection: %s", exc, exc_info=True)

        except asyncio.CancelledError:
            # Graceful shutdown on application stop
            if connection is not None:
                try:
                    await connection.close()
                except Exception as exc:
                    logger.error("Error while closing RabbitMQ connection on cancel: %s", exc, exc_info=True)
            raise
        except Exception as exc:
            # Log unexpected errors and retry with exponential backoff
            logger.error("RabbitMQ consumer error: %s", exc, exc_info=True)
            await asyncio.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2, 30)
