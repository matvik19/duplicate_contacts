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
        log = logger.bind(queue=self.queue_name, subdomain=subdomain)

        try:
            log.info("Получение настроек дублей для subdomain={}", subdomain)

            settings = await self.duplicate_settings_service.get_duplicate_settings(
                session, subdomain
            )

            if data["reply_to"]:
                await self.rmq_publisher.send_response(
                    json.dumps(settings), data["reply_to"], data["correlation_id"]
                )
                log.info("📤 Настройки отправлены в очередь {}", data["reply_to"])
        except Exception:
            log.exception("❌ Ошибка при получении или отправке настроек дублей")
            raise
