import json
import asyncio
import aio_pika
from abc import ABC, abstractmethod
from loguru import logger

from src.common.database import DatabaseManager
from src.rabbitmq.rmq_connetcion import RMQConnectionManager
from src.rabbitmq.rmq_publisher import RMQPublisher

MAX_RETRIES = 5


class BaseConsumer(ABC):
    def __init__(
        self,
        queue_name: str,
        connection_manager: RMQConnectionManager,
        rmq_publisher: RMQPublisher,
        db_manager: DatabaseManager,
    ):
        self.queue_name = queue_name
        self.connection_manager = connection_manager
        self.rmq_publisher = rmq_publisher
        self.db_manager = db_manager

    async def start(self):
        """Запускает консьюмера"""
        while True:
            try:
                async with self.connection_manager as connection:
                    channel = await connection.channel()
                    await channel.set_qos(prefetch_count=10)
                    queue = await channel.get_queue(self.queue_name)

                    async with queue.iterator() as queue_iter:
                        async for message in queue_iter:
                            await self.process_message(message)
            except asyncio.CancelledError:
                logger.warning(f"Консьюмер {self.queue_name} отменен.")
                break
            except Exception as e:
                logger.error(f"Ошибка в работе консьюмера {self.queue_name}: {e}")
                await asyncio.sleep(10)

    async def process_message(self, message: aio_pika.IncomingMessage):
        """Обрабатывает сообщение"""
        retry_count = message.headers.get("x-retry", 0)
        try:
            body = message.body.decode("utf-8")
            data = json.loads(body)
            logger.info(f"[{self.queue_name}] Получено сообщение: {data}")

            async with self.db_manager.get_session() as session:
                async with session.begin():
                    await self.handle_message(data, session)

            await message.ack()
        except json.JSONDecodeError:
            logger.error("Ошибка: Некорректный JSON в сообщении")
            await message.reject(requeue=False)
        except Exception as e:
            logger.error(f"Ошибка обработки сообщения: {e}")
            if retry_count >= MAX_RETRIES:
                logger.error("Достигнут лимит повторных попыток. Отправляем в DLX.")
                await message.reject(requeue=False)
            else:
                logger.warning(
                    f"Попытка обработки не удалась. Републикация №{retry_count + 1}"
                )
                await self.rmq_publisher.republish_message(message, retry_count + 1)
                await message.ack()

    @abstractmethod
    async def handle_message(self, data: dict, session):
        """Абстрактный метод для обработки данных"""
        pass
