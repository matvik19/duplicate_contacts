import json
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession

from src.duplicate_contact.schemas import ContactDuplicateSettingsSchema
from src.duplicate_contact.services.duplicate_settings import (
    DuplicateSettingsService,
)
from src.rabbitmq.consumers.base_consumer import BaseConsumer


class SaveSettingsConsumer(BaseConsumer):
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
        subdomain = data.get("subdomain")
        log = logger.bind(queue=self.queue_name, subdomain=subdomain)

        try:
            log.info("Сохранение настроек дублей")
            settings_data = ContactDuplicateSettingsSchema(**data)

            await self.duplicate_settings_service.add_duplicate_settings(
                session, settings_data
            )
            log.info("Настройки дублей сохранены.")

        except Exception:
            log.exception("Ошибка при сохранении настроек дублей")
            raise
