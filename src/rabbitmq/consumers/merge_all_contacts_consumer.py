import json
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession

from src.common.exceptions import (
    AmoCRMServiceError,
    NetworkError,
    SettingsNotFoundError,
    ProcessingError,
    ValidationError,
)
from src.common.token_service import TokenService
from src.duplicate_contact.services.contact_merge_service import ContactMergeService
from src.duplicate_contact.services.duplicate_settings import (
    DuplicateSettingsService,
)
from src.rabbitmq.consumers.base_consumer import BaseConsumer


class MergeAllContactsConsumer(BaseConsumer):
    """Консьюмер для обработки дублей контактов."""

    def __init__(
        self,
        queue_name: str,
        connection_manager,
        rmq_publisher,
        db_manager,
        duplicate_service: ContactMergeService,
        token_service: TokenService,
        duplicate_settings_service: DuplicateSettingsService,
    ):
        super().__init__(queue_name, connection_manager, rmq_publisher, db_manager)
        self.duplicate_service = duplicate_service
        self.token_service = token_service
        self.duplicate_settings_service = duplicate_settings_service

    async def handle_message(self, data: dict, session: AsyncSession):
        subdomain = data.get("subdomain")
        log = logger.bind(queue=self.queue_name, subdomain=subdomain)
        if not subdomain:
            raise ValidationError("Отсутствует subdomain в сообщении")

        try:
            log.info("Начало объединения всех дублей")
            access_token = await self.token_service.get_tokens(subdomain)
            settings = await self.duplicate_settings_service.get_duplicate_settings(
                session, subdomain
            )

            if not settings.merge_is_active:
                log.info("Слияние отключено в настройках")
                return

            await self.duplicate_service.merge_all_contacts(
                settings, access_token, session
            )
            log.info("Объединение завершено")
        except Exception as e:
            raise ProcessingError(f"Ошибка объединения всех контактов. Error: {e}")
