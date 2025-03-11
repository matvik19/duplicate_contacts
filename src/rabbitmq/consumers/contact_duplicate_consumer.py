import json
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession
from src.rabbitmq.consumers.base_consumer import BaseConsumer


class ContactDuplicateConsumer(BaseConsumer):
    """Консьюмер для обработки дублей контактов."""

    def __init__(
        self,
        queue_name: str,
        connection_manager,
        rmq_publisher,
        db_manager,
        duplicate_service: ContactDuplicateService,
    ):
        super().__init__(queue_name, connection_manager, rmq_publisher, db_manager)
        self.duplicate_service = duplicate_service  # ✅ Просто сохраняем сервис

    async def handle_message(self, data: dict, session: AsyncSession):
        """Обрабатывает сообщение с дублями контактов."""
        try:
            logger.info(
                f"📩 Получено сообщение о дублях контактов: {json.dumps(data, indent=2)}"
            )

            # ✅ Теперь просто вызываем сервис дублей
            await self.duplicate_service.process_contact_duplicates(data, session)

            logger.info("✅ Дубли контактов успешно обработаны.")
        except Exception as e:
            logger.error(f"❌ Ошибка обработки дублей контактов: {e}")
            raise
