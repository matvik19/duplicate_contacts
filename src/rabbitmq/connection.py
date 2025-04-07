import aio_pika
from loguru import logger

from src.common.exceptions import NetworkError


class RMQConnectionManager:
    """Класс для управления соединением с RabbitMQ."""

    def __init__(self, connection_url: str):
        self.connection_url = connection_url
        self.connection = None

    async def connect(self):
        """Создаёт и возвращает соединение с RabbitMQ."""
        if not self.connection or self.connection.is_closed:
            try:
                self.connection = await aio_pika.connect_robust(
                    self.connection_url, timeout=10
                )
                logger.info("Подключение к RabbitMQ успешно установлено.")
            except Exception as e:
                raise NetworkError(f"Ошибка подключения к RabbitMQ. Error: {e}")
        return self.connection

    async def close(self):
        """Закрывает соединение, если оно открыто."""
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
            logger.info("Соединение с RabbitMQ закрыто.")

    async def __aenter__(self):
        """Контекстный менеджер для автоматического открытия соединения."""
        return await self.connect()

    async def __aexit__(self, exc_type, exc_value, traceback):
        """Закрывает соединение при выходе из контекста."""
        await self.close()
