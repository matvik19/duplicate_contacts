from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession

from src.common.exceptions import AmoCRMServiceError
from src.common.token_service import TokenService
from src.duplicate_contact.services.exclusion import ContactExclusionService
from src.rabbitmq.consumers.base_consumer import BaseConsumer


class ExclusionConsumer(BaseConsumer):
    """Консьюмер для обработки дублей контактов."""

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
        """Обрабатывает сообщение с дублями контактов."""
        subdomain = data["subdomain"]
        contact_id = data["contact_id"]
        log = logger.bind(
            queue=self.queue_name, subdomain=subdomain, contact_id=contact_id
        )
        try:
            log.info("Обработка исключений для контакта.")
            access_token = await self.token_service.get_tokens(data["subdomain"])

            result = await self.exclusion_service.add_contact_to_exclusion(
                session, subdomain, contact_id, access_token
            )
            if result.get("success"):
                log.info("Исключения добавлены: {}", result.get("added_exclusions"))
            else:
                log.warning("Контакт не добавлен в исключения: {}", result)
        except AmoCRMServiceError as e:
            log.error("Ошибка получения токена: {}", e)
            raise
        except Exception:
            log.exception("Ошибка при добавлении исключений для контакта")
            raise
