import json
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession

from src.duplicate_contact.schemas import ContactDuplicateSettingsSchema
from src.duplicate_contact.services.duplicate_settings import (
    DuplicateSettingsService,
)
from src.rabbitmq.consumers.base_consumer import BaseConsumer


class GetSettingsConsumer(BaseConsumer):
    """Консьюмер для обработки дублей контактов."""

    def __init__(
        self,
        queue_name: str,
        connection_manager,
        rmq_publisher,
        db_manager,
        duplicate_settings_service: DuplicateSettingsService,
    ):
        super().__init__(queue_name, connection_manager, rmq_publisher, db_manager)
        self.duplicate_settings_service = duplicate_settings_service

    async def handle_message(self, data: dict, session: AsyncSession):
        """Обрабатывает сообщение с дублями контактов."""
        subdomain = data["subdomain"]
        reply_to = data["reply_to"]
        correlation_id = data["correlation_id"]

        try:
            settings = await self.duplicate_settings_service.get_duplicate_settings(
                session, subdomain
            )
            logger.info(
                f"Получены настройки на дубли контактов: {json.dumps(data, indent=2)}"
            )

            # Если получен параметр reply_to, отправляем ответ
            if reply_to:
                # Отправляем настройки обратно в очередь
                await self.rmq_publisher.send_response(
                    json.dumps(settings), reply_to, correlation_id
                )
                logger.info(f"Ответ отправлен в очередь {reply_to}.")
            logger.info("Отдали настройки")
        except Exception as e:
            logger.error(f"❌ Ошибка при возврате настроек для {subdomain}: {e}")
            raise
