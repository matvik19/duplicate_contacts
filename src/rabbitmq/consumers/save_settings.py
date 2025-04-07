import json
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession

from src.common.exceptions import ValidationError, ProcessingError, NetworkError
from src.duplicate_contact.schemas import ContactDuplicateSettingsSchema
from src.duplicate_contact.services.duplicate_settings import DuplicateSettingsService
from src.rabbitmq.consumers.base_consumer import BaseConsumer


class SaveSettingsConsumer(BaseConsumer):
    """Консьюмер для сохранения настроек дублей."""

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
        """Обрабатывает сообщение для сохранения настроек дублей."""
        subdomain = data.get("subdomain")
        log = logger.bind(queue=self.queue_name, subdomain=subdomain)

        if not subdomain:
            raise ValidationError("Subdomain обязателен в сообщении")

        try:
            log.info("Начало сохранения настроек дублей для subdomain={}", subdomain)
            settings_data = ContactDuplicateSettingsSchema(**data)
            log.debug("Данные настроек успешно валидированы")

            await self.duplicate_settings_service.add_duplicate_settings(
                session, settings_data
            )
            log.info("Настройки дублей успешно сохранены")

        except Exception as e:
            raise ProcessingError(f"Ошибка сохранения настроек. Error: {e}")
