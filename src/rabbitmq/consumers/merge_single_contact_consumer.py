import json
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession

from src.common.exceptions import AmoCRMServiceError
from src.common.token_service import TokenService
from src.duplicate_contact.services.contact_merge_service import ContactMergeService
from src.duplicate_contact.services.duplicate_settings import (
    DuplicateSettingsService,
)
from src.rabbitmq.consumers.base_consumer import BaseConsumer


class MergeSingleContactConsumer(BaseConsumer):
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
        """Обрабатывает сообщение с дублями контактов."""
        subdomain = data.get("subdomain")
        contact_id = data.get("contact_id")
        log = logger.bind(
            queue=self.queue_name, subdomain=subdomain, contact_id=contact_id
        )

        try:
            log.info("Обработка дублей для одного контакта.")
            access_token = await self.token_service.get_tokens(subdomain)
            settings = await self.duplicate_settings_service.get_duplicate_settings(
                session, subdomain
            )

            if not settings:
                log.warning("Настройки дублей не найдены.")
                return

            if not settings.merge_is_active:
                log.warning("Слияние отключено в настройках.")
                return

            await self.duplicate_service.merge_single_contact(
                settings, access_token, contact_id, session
            )
            log.info("✅ Контакт успешно объединён.")
        except AmoCRMServiceError as e:
            log.error("Ошибка получения токена: {}", e)
            raise
        except Exception:
            log.exception("Ошибка при объединении дубля одного контакта")
            raise
