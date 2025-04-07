import json
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession

from src.common.exceptions import (
    SettingsNotFoundError,
    ProcessingError,
    NetworkError,
    ValidationError,
)
from src.duplicate_contact.schemas import ContactDuplicateSettingsSchema
from src.duplicate_contact.services.duplicate_settings import DuplicateSettingsService
from src.rabbitmq.consumers.base_consumer import BaseConsumer


class GetSettingsConsumer(BaseConsumer):
    """Консьюмер для получения настроек дублей."""

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
        """Обрабатывает сообщение для получения настроек дублей."""
        subdomain = data.get("subdomain")
        log = logger.bind(queue=self.queue_name, subdomain=subdomain)

        if not subdomain:
            log.error("Отсутствует subdomain в сообщении")
            raise ValidationError("Subdomain обязателен в сообщении")

        try:
            log.info("Получение настроек дублей")
            settings = await self.duplicate_settings_service.get_duplicate_settings(
                session, subdomain
            )

            if data.get("reply_to"):
                settings_json = json.dumps(
                    settings.model_dump()
                    if isinstance(settings, ContactDuplicateSettingsSchema)
                    else settings
                )
                await self.rmq_publisher.send_response(
                    settings_json, data["reply_to"], data.get("correlation_id")
                )
                log.info("Настройки отправлены в очередь {}", data["reply_to"])
            else:
                log.debug("Настройки получены, но reply_to не указан")

        except Exception as e:
            raise ProcessingError(f"Ошибка обработки сообщения. Error: {e}")
