from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession

from src.amocrm.service import AmocrmService
from src.common.exceptions import AmoCRMServiceError
from src.duplicate_contact.repository import ContactDuplicateRepository
from src.duplicate_contact.schemas import ContactDuplicateSettingsSchema
from src.duplicate_contact.services.base import ContactService
from src.duplicate_contact.services.find_duplicate import DuplicateFinderService
from src.duplicate_contact.utils.prepare_merge_data import prepare_merge_data


class ContactMergeService(ContactService):
    """Сервис для склейки дублей контактов."""

    def __init__(
        self,
        find_duplicate_service: DuplicateFinderService,
        duplicate_repo: ContactDuplicateRepository,
        amocrm_service: AmocrmService,
    ):
        super().__init__(amocrm_service)
        self.find_duplicate_service = find_duplicate_service
        self.duplicate_repo = duplicate_repo

    async def merge_all_contacts(
        self,
        settings: ContactDuplicateSettingsSchema,
        access_token: str,
        session: AsyncSession,
    ) -> list[dict[str, any]]:
        """Объединяет все группы дублей контактов."""
        groups = await self.find_duplicate_service.find_duplicates_all_contacts(
            subdomain=settings.subdomain,
            access_token=access_token,
            blocks=settings.keys,
            merge_all=settings.merge_all,
        )
        logger.info(f"Найдено групп дублей: {len(groups)}")
        if not groups:
            logger.info("Дубли не найдены.")
            return []

        return [
            result
            async for result in self._process_groups(
                groups, settings, access_token, session
            )
            if result
        ]

    async def merge_single_contact(
        self,
        settings: ContactDuplicateSettingsSchema,
        access_token: str,
        contact_id: int,
        session: AsyncSession,
    ) -> dict[str, any]:
        """Объединяет дубли для одного контакта."""
        group = await self.find_duplicate_service.find_duplicates_single_contact(
            subdomain=settings.subdomain,
            access_token=access_token,
            contact_id=contact_id,
            blocks=settings.keys,
            merge_all=settings.merge_all,
        )
        if not group or len(group.get("group", [])) < 2:
            logger.info(f"Дубли не найдены для контакта {contact_id}.")
            return {}

        result = await self._merge_contact_group(group, settings, access_token, session)
        return result or {}

    async def _process_groups(
        self,
        groups: list[dict[str, any]],
        settings: ContactDuplicateSettingsSchema,
        access_token: str,
        session: AsyncSession,
    ):
        """Генератор для обработки групп дублей."""
        for group_data in groups:
            if len(group_data.get("group", [])) >= 2:
                yield await self._merge_contact_group(
                    group_data, settings, access_token, session
                )

    async def _merge_contact_group(
        self,
        group_data: dict[str, any],
        settings: ContactDuplicateSettingsSchema,
        access_token: str,
        session: AsyncSession,
    ) -> dict[str, any] | None:
        """Сливает группу дублей."""
        group = group_data["group"]
        matched_block_db_id = group_data.get("matched_block_db_id")
        main_contact, *duplicates = group
        contact_ids = [c["id"] for c in group]

        try:
            payload = await prepare_merge_data(
                main_contact, duplicates, settings.priority_fields
            )
            logger.info(f"Подготовлен payload для слияния: {payload}")

            merge_response = await self.amocrm_service.merge_contacts(
                subdomain=settings.subdomain,
                access_token=access_token,
                result_element=payload,
            )
            if not merge_response:
                raise AmoCRMServiceError(
                    f"Не удалось выполнить слияние для группы {contact_ids}, subdomain: {settings.subdomain}"
                )
            logger.info(f"Слияние успешно для контактов: {[c['id'] for c in group]}")

            await self._add_merged_tag(
                settings.subdomain, access_token, main_contact["id"], payload
            )
            if matched_block_db_id:
                await self.duplicate_repo.insert_merge_block_log(
                    session, settings.subdomain, matched_block_db_id, main_contact["id"]
                )

            return merge_response
        except Exception as e:
            logger.error(
                f"Ошибка слияния в _merge_contact_group для {settings.subdomain}: {e}"
            )
            return None

    async def _add_merged_tag(
        self,
        subdomain: str,
        access_token: str,
        contact_id: int,
        payload: dict[str, any],
    ) -> None:
        """Добавляет тег 'merged' к контакту."""
        tags = payload.get("result_element[TAGS][]", [])
        await self.amocrm_service.add_tag_merged_to_contact(
            subdomain=subdomain,
            access_token=access_token,
            contact_id=contact_id,
            all_tags=tags,
        )
