from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession

from src.common.exceptions import (
    AmoCRMServiceError,
    ValidationError,
    NetworkError,
    ProcessingError,
    TokenError,
)
from src.common.token_service import TokenService
from src.duplicate_contact.services.exclusion import ContactExclusionService
from src.rabbitmq.consumers.base_consumer import BaseConsumer


class ExclusionConsumer(BaseConsumer):
    """Консьюмер для добавления исключений из контактов."""

    def __init__(
        self,
        queue_name: str,
        connection_manager,
        rmq_publisher,
        db_manager,
        exclusion_service: ContactExclusionService,
        token_service: TokenService,
    ):
        super().__init__(queue_name, connection_manager, rmq_publisher, db_manager)
        self.exclusion_service = exclusion_service
        self.token_service = token_service

    async def handle_message(self, data: dict, session: AsyncSession):
        """Обрабатывает сообщение для добавления исключений."""
        subdomain = data.get("subdomain")
        contact_id = data.get("contact_id")
        log = logger.bind(
            queue=self.queue_name, subdomain=subdomain, contact_id=contact_id
        )

        if not subdomain or not contact_id:
            raise ValidationError("Отсутствует subdomain или contact_id в сообщении")

        try:
            log.info("Начало обработки исключений для контакта")
            access_token = await self.token_service.get_tokens(subdomain)
            result = await self.exclusion_service.add_contact_to_exclusion(
                session, subdomain, contact_id, access_token
            )
            if result.get("success"):
                log.info("Исключения добавлены: {}", result.get("added_exclusions"))
            else:
                log.error("Контакт не добавлен в исключения: {}", result)

        except Exception as e:
            log.exception("Неизвестная ошибка при добавлении исключений: {}", e)
            raise ProcessingError(
                f"Ошибка обработки исключений для subdomain={subdomain}, contact_id={contact_id}"
            )
