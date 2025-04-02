from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession
from src.duplicate_contact.repository import ContactDuplicateRepository
from src.duplicate_contact.services.find_duplicate import DuplicateFinderService
from .base import ContactService
from ...amocrm.service import AmocrmService


class ContactExclusionService(ContactService):
    """Сервис для добавления исключений из контактов."""

    def __init__(
        self,
        duplicate_repo: ContactDuplicateRepository,
        amocrm_service: AmocrmService,
        find_duplicate_service: DuplicateFinderService,
    ):
        super().__init__(amocrm_service)
        self.duplicate_repo = duplicate_repo
        self.find_duplicate_service = find_duplicate_service

    async def add_contact_to_exclusion(
        self, session: AsyncSession, subdomain: str, contact_id: int, access_token: str
    ) -> dict[str, any]:
        """Добавляет значения полей контакта в исключения на основе лога склейки."""
        merge_log = await self.duplicate_repo.get_merge_log_by_contact_and_subdomain(
            session, contact_id, subdomain
        )
        if not merge_log:
            logger.error(
                f"Лог склейки не найден для contact_id {contact_id} и subdomain {subdomain}"
            )
            return {"error": "Лог склейки не найден"}

        block = await self.duplicate_repo.get_block_by_id(session, merge_log.block_id)
        if not block:
            logger.error(f"Блок с id {merge_log.block_id} не найден")
            return {"error": "Блок не найден"}

        contact = await self.get_contact(subdomain, access_token, contact_id)
        if not contact:
            return {"error": "Контакт не найден"}

        added_exclusions = await self._add_exclusions(session, contact, block.fields)
        await session.commit()
        logger.info(
            f"Для контакта {contact_id} добавлены исключения: {added_exclusions}"
        )
        return {"success": True, "added_exclusions": added_exclusions}

    async def _add_exclusions(
        self, session: AsyncSession, contact: dict[str, any], fields: list[any]
    ) -> list[dict[str, any]]:
        """Добавляет значения полей в исключения."""
        exclusions = []
        for field in fields:
            value = self.find_duplicate_service.extract_field_value_simple(
                contact, field.field_name
            )
            if value:
                await self.duplicate_repo.insert_exclusion_values(
                    session, field.id, field.field_name, [{"value": value}]
                )
                exclusions.append({"field_name": field.field_name, "value": value})
        return exclusions
