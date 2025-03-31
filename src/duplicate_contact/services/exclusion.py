from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession
from src.duplicate_contact.repository import ContactDuplicateRepository
from src.amocrm.service import AmocrmService
from src.duplicate_contact.services.find_duplicate import FindDuplicateService


class ExclusionService:
    def __init__(
        self,
        duplicate_repo: ContactDuplicateRepository,
        amocrm_service: AmocrmService,
    ):
        self.duplicate_repo = duplicate_repo
        self.amocrm_service = amocrm_service
        # Используем существующий сервис для извлечения значений полей
        self.find_duplicate_service = FindDuplicateService(amocrm_service)

    async def add_contact_to_exclusion(
        self, session: AsyncSession, subdomain: str, contact_id: int, access_token: str
    ) -> dict:
        """
        Метод добавления значений полей контакта в исключения.
        1. По contact_id и subdomain ищется запись лога склейки.
        2. Получается блок, по которому была выполнена склейка.
        3. Из контакта извлекаются значения полей (например, Телефон, Почта, Должность).
        4. Для каждого поля добавляется запись в таблицу exclusion_fields.
        """
        # 1. Получаем лог склейки
        merge_log = await self.duplicate_repo.get_merge_log_by_contact_and_subdomain(
            session, contact_id, subdomain
        )
        if not merge_log:
            logger.error(
                f"Лог склейки не найден для contact_id {contact_id} и subdomain {subdomain}"
            )
            return {"error": "Лог склейки не найден"}

        block_db_id = merge_log.block_id
        print("merge_log", merge_log.created_at)

        # 2. Получаем данные блока с полями
        block = await self.duplicate_repo.get_block_by_id(session, block_db_id)
        if not block:
            logger.error(f"Блок с id {block_db_id} не найден")
            return {"error": "Блок не найден"}

        # 3. Получаем данные контакта из amoCRM
        contact = await self.amocrm_service.get_contact_by_id(
            subdomain, access_token, contact_id
        )
        if not contact:
            logger.error(f"Контакт с id {contact_id} не найден в amoCRM")
            return {"error": "Контакт не найден"}

        # 4. Для каждого поля блока извлекаем значение из контакта и добавляем в исключения
        added_exclusions = []
        for block_field in block.fields:
            field_name = block_field.field_name
            # Используем метод для извлечения значения поля (аналог _extract_field_value_simple)
            field_value = self.find_duplicate_service.extract_field_value_simple(
                contact, field_name
            )
            if field_value:
                # Добавляем значение в исключения для данного поля.
                # Метод insert_exclusion_values ожидает список словарей вида [{"value": значение}]
                await self.duplicate_repo.insert_exclusion_values(
                    session,
                    block_field_id=block_field.id,
                    field_name=field_name,
                    exclusion_fields=[{"value": field_value}],
                )
                added_exclusions.append(
                    {"field_name": field_name, "value": field_value}
                )

        await session.commit()
        logger.info(
            f"Для контакта {contact_id} добавлены исключения: {added_exclusions}"
        )
        return {"success": True, "added_exclusions": added_exclusions}
