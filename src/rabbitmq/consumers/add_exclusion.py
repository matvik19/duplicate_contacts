import json

from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession

from src.common.token_service import TokenService
from src.duplicate_contact.services.exclusion import ExclusionService
from src.rabbitmq.consumers.base_consumer import BaseConsumer


class ExclusionConsumer(BaseConsumer):
    """Консьюмер для обработки дублей контактов."""

    def __init__(
        self,
        queue_name: str,
        connection_manager,
        rmq_publisher,
        db_manager,
        exclusion_service: ExclusionService,
        token_service: TokenService,
    ):
        super().__init__(queue_name, connection_manager, rmq_publisher, db_manager)
        self.exclusion_service = exclusion_service
        self.token_service = token_service

    async def handle_message(self, data: dict, session: AsyncSession):
        """Обрабатывает сообщение с дублями контактов."""
        try:
            logger.info(
                f"Получены настройки на дубли контактов: {json.dumps(data, indent=2)}"
            )
            subdomain = data["subdomain"]
            contact_id = int(data["contact_id"])
            access_token = await self.token_service.get_tokens(data["subdomain"])

            await self.exclusion_service.add_contact_to_exclusion(
                session, subdomain, contact_id, access_token
            )

            logger.info("✅ Дубли контактов успешно обработаны.")
        except Exception as e:
            logger.error(f"❌ Ошибка обработки дублей контактов: {e}")
            raise
