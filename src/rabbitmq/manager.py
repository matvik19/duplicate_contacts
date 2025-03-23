import asyncio
import aio_pika
from loguru import logger
from src.common.database import DatabaseManager
from src.rabbitmq.connection import RMQConnectionManager
from src.rabbitmq.publisher import RMQPublisher


class RMQManager:
    """Класс для управления RabbitMQ: настройка очередей, запуск консьюмеров."""

    def __init__(
        self,
        connection_manager: RMQConnectionManager,
        db_manager: DatabaseManager,
        rmq_publisher: RMQPublisher,
        consumers: list,
    ):
        self.connection_manager = connection_manager
        self.db_manager = db_manager
        self.rmq_publisher = rmq_publisher
        self.consumers = consumers

    async def setup_rabbitmq(self):
        """Настройка всех очередей и Dead Letter Exchange (DLX)."""
        async with self.connection_manager as connection:
            channel = await connection.channel()

            # Создаём DLX
            dlx_exchange = await channel.declare_exchange(
                "dlx_exchange_duplicate",
                type=aio_pika.ExchangeType.DIRECT,
                durable=True,
            )

            # Определяем мёртвые очереди для каждой основной очереди
            dead_letter_queues = {
                "merge_duplicates_all_contacts": "dead_letter_merge_duplicates_all_contacts",
                "save_contact_duplicates_settings": "dead_letter_save_contact_duplicates_settings",
                "merge_duplicates_single_contact": "dead_letter_merge_duplicates_single_contact",
            }

            # Создаем и связываем DLX
            for queue_name, dlq_name in dead_letter_queues.items():
                dead_letter_queue = await channel.declare_queue(dlq_name, durable=True)
                await dead_letter_queue.bind(dlx_exchange, routing_key=dlq_name)

            # Создаем основные очереди с DLX
            for queue_name, dlq_name in dead_letter_queues.items():
                await channel.declare_queue(
                    queue_name,
                    durable=True,
                    arguments={
                        "x-dead-letter-exchange": "dlx_exchange_duplicate",
                        "x-dead-letter-routing-key": dlq_name,
                        "x-message-ttl": 60000,  # TTL 60 секунд
                        "x-max-length": 1000,  # Максимальная длина очереди
                    },
                )

            logger.info("✅ RabbitMQ: все очереди и DLX настроены.")

    async def start_all_consumers(self):
        """Запускает все консьюмеры."""
        await self.setup_rabbitmq()  # Создаём очереди перед запуском консьюмеров

        logger.info("Запуск всех консьюмеров...")

        # Запускаем всех консьюмеров асинхронно
        await asyncio.gather(*(consumer.start() for consumer in self.consumers))
