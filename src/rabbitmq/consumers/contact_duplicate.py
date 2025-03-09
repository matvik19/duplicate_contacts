import json
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession
from src.rabbitmq.base_consumer import BaseConsumer


class ContactDuplicateConsumer(BaseConsumer):
    """Консьюмер для обработки дублей контактов."""

    async def handle_message(self, data: dict, session: AsyncSession):
        """
        Обрабатывает сообщение с данными о дублях контактов.

        :param data: Данные из RabbitMQ-сообщения
        :param session: Асинхронная сессия SQLAlchemy для работы с БД
        """
        try:
            logger.info(f"📩 Получено сообщение о дублях контактов: {json.dumps(data, indent=2)}")

            # Здесь вызываем логику обработки дублей контактов
            await self.process_contact_duplicates(data, session)

            logger.info("✅ Дубли контактов успешно обработаны.")
        except Exception as e:
            logger.error(f"❌ Ошибка обработки дублей контактов: {e}")
            raise

    @staticmethod
    async def process_contact_duplicates(data: dict, session: AsyncSession):
        """
        Логика обработки дублей контактов.

        :param data: Данные из RabbitMQ-сообщения
        :param session: Асинхронная сессия SQLAlchemy
        """
        contact_ids = data.get("contact_ids", [])
        if not contact_ids:
            logger.warning("⚠️ В сообщении отсутствуют ID контактов для объединения.")
            return

        # Здесь должна быть логика объединения дублей в базе данных
        logger.info(f"🔄 Начинаем объединение дублей: {contact_ids}")

        # Пример: Запрос к БД
        # result = await session.execute(<SQL-запрос на объединение дублей>)
        # await session.commit()

        logger.info("✅ Контакты успешно объединены.")
