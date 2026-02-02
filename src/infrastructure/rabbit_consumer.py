import asyncio
import json
import logging
from typing import Any

import aio_pika

from src.infrastructure.config import Settings
from src.services.consumer_service import BrokerServicesDTO, handle_message

logger = logging.getLogger(__name__)

class RabbitHandler:
    """Basic RabbitMQ handler to start the connection and handle messages."""

    def __init__(self, services: BrokerServicesDTO, settings: Settings) -> None:
        self.settings = settings
        self.services = services

    async def _handle_message(self, message: aio_pika.IncomingMessage) -> None:
        """General message handler to download the file from message's URL,
        parse it and send to database
        """
        async with message.process(requeue=False):
            body = message.body.decode("utf-8")
            try:
                payload: dict[str, Any] = json.loads(body)
                url = payload["file"]
            except Exception as exc:
                logger.error("Invalid message: %s - error: %s", body, exc)
                return

            try:
                # Use the common message handler
                handle_message(url, self.services, self.settings.download_timeout_seconds)
            except Exception as exc:
                logger.exception("Failed processing file %s: %s", url, exc)
                # Do not requeue to avoid hot-loop; DLQ should be configured at broker level


    async def run_consumer(self) -> None:
        """Run RabbitMQ consumer in an endless loop with reconnect/backoff on errors.

        All unexpected errors are logged with ERROR level. Shutdown via task cancel
        is handled gracefully.
        """
        backoff_seconds = 1
        connection = None
        while True:
            try:
                connection = await aio_pika.connect_robust(self.settings.rabbitmq_url)
                logger.info(
                    "RabbitMQ connected: url=%s, queue=%s, prefetch=%s",
                    self.settings.rabbitmq_url,
                    self.settings.rabbitmq_queue,
                    self.settings.rabbitmq_prefetch,
                )

                channel = await connection.channel()
                await channel.set_qos(prefetch_count=self.settings.rabbitmq_prefetch)
                queue = await channel.declare_queue(self.settings.rabbitmq_queue, durable=True)
                await queue.consume(self._handle_message)

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
