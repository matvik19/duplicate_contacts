import json
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession

from src.common.token_service import TokenService
from src.duplicate_contact.services.duplicate_settings import (
    DuplicateSettingsService,
)
from src.duplicate_contact.services.merge_duplicate import MergeAllContacts
from src.rabbitmq.consumers.base_consumer import BaseConsumer


class MergeAllContactsConsumer(BaseConsumer):
    """Консьюмер для обработки дублей контактов."""

    def __init__(
        self,
        queue_name: str,
        connection_manager,
        rmq_publisher,
        db_manager,
        duplicate_service: MergeAllContacts,
        token_service: TokenService,
        duplicate_settings_service: DuplicateSettingsService,
    ):
        super().__init__(queue_name, connection_manager, rmq_publisher, db_manager)
        self.duplicate_service = duplicate_service
        self.token_service = token_service
        self.duplicate_settings_service = duplicate_settings_service

    async def handle_message(self, data: dict, session: AsyncSession):
        """Обрабатывает сообщение с дублями контактов."""
        try:
            subdomain = data["subdomain"]
            access_token = await self.token_service.get_tokens(data["subdomain"])
            duplicate_settings = (
                await self.duplicate_settings_service.get_duplicate_settings(
                    session, subdomain
                )
            )
            if not duplicate_settings:
                logger.info(f"Настройки дублей не найдены для subdomain: {subdomain}")
                return

            if not duplicate_settings.merge_is_active:
                logger.warning(
                    f"Объединение контактов выключено в настройках для: {subdomain}"
                )
                return

            # ✅ Теперь просто вызываем сервис дублей
            await self.duplicate_service.merge_duplicates(
                duplicate_settings,
                access_token,
            )

            logger.info("✅ Дубли контактов успешно обработаны.")
        except Exception as e:
            logger.error(
                f"❌LJNFK:JLASNFNJASNFDJKNASKJDFNKASJDNJA Ошибка обработки дублей контактов: {e}"
            )
            raise
