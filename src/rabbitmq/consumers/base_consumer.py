import json
import asyncio
import aio_pika
from abc import ABC, abstractmethod
from loguru import logger

from src.common.database import DatabaseManager
from src.common.exceptions import (
    ValidationError,
    SettingsNotFoundError,
    ProcessingError,
    NetworkError,
    TokenError,
    AmoCRMServiceError,
)
from src.rabbitmq.connection import RMQConnectionManager
from src.rabbitmq.publisher import RMQPublisher

MAX_RETRIES = 1


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
                connection = await self.connection_manager.connect()
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
        retry_count = message.headers.get("x-retry", 0)
        log = logger.bind(queue=self.queue_name)
        try:
            body = message.body.decode("utf-8")
            data = json.loads(body)
            log = log.bind(subdomain=data.get("subdomain"))
            log.info(f"Получено сообщение: {data}")

            async with self.db_manager.get_session() as session:
                async with session.begin():
                    await self.handle_message(data, session)

            await message.ack()
            log.debug("Сообщение успешно обработано")
        except json.JSONDecodeError as e:
            log.error(f"Некорректный JSON: {e}")
            await message.reject(requeue=False)
        except (NetworkError, TokenError) as e:
            log.error(f"Ошибка с retry: {e}")
            if retry_count >= MAX_RETRIES:
                log.error("Лимит повторов исчерпан, отправка в DLX")
                await message.reject(requeue=False)
            else:
                log.warning(f"Повторная попытка #{retry_count + 1}")
                await self.rmq_publisher.republish_message(message, retry_count + 1)
                await message.ack()
        except AmoCRMServiceError as e:
            log.error("Ошибка API amoCRM: {}", e)
            await message.reject(requeue=False)
        except (ValidationError, SettingsNotFoundError, ProcessingError) as e:
            log.error(f"Логическая ошибка: {e}")
            await message.reject(requeue=False)
        except Exception as e:
            log.exception(f"Неизвестная ошибка: {e}")
            await message.reject(requeue=False)

    @abstractmethod
    async def handle_message(self, data: dict, session):
        """Абстрактный метод для обработки данных"""
        pass
