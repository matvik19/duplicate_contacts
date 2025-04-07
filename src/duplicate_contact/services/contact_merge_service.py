from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession

from src.amocrm.service import AmocrmService
from src.common.exceptions import AmoCRMServiceError, NetworkError, ProcessingError
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
        log = logger.bind(subdomain=settings.subdomain)
        try:
            groups = await self.find_duplicate_service.find_duplicates_all_contacts(
                subdomain=settings.subdomain,
                access_token=access_token,
                blocks=settings.keys,
                merge_all=settings.merge_all,
            )
            if not groups:
                log.info("Дубли не найдены для объединения.")
                return []

            log.info(f"Найдено {len(groups)} групп дублей для обработки")
            results = [
                result
                async for result in self._process_groups(
                    groups, settings, access_token, session
                )
                if result
            ]
            log.info(f"Обработано {len(results)} групп дублей")
            return results
        except NetworkError:
            log.error("Сетевая ошибка при поиске дублей")
            raise  # Для retry
        except Exception as e:
            log.exception(f"Неизвестная ошибка при объединении всех контактов: {e}")
            raise ProcessingError("Ошибка обработки дублей")

    async def merge_single_contact(
        self,
        settings: ContactDuplicateSettingsSchema,
        access_token: str,
        contact_id: int,
        session: AsyncSession,
    ) -> dict[str, any]:
        log = logger.bind(subdomain=settings.subdomain, contact_id=contact_id)
        try:
            group = await self.find_duplicate_service.find_duplicates_single_contact(
                subdomain=settings.subdomain,
                access_token=access_token,
                target_contact_id=contact_id,
                blocks=settings.keys,
                merge_all=settings.merge_all,
            )
            if not group or len(group.get("group", [])) < 2:
                log.debug("Дубли не найдены для одного контакта")
                return {}

            contact_ids = [c["id"] for c in group.get("group", [])]
            log.info(
                f"Найдена группа для объединения: {len(contact_ids)} контактов → {contact_ids}"
            )
            result = await self._merge_contact_group(
                group, settings, access_token, session
            )
            return result or {}
        except Exception as e:
            log.exception(f"Ошибка при объединении контакта: {e}")
            raise ProcessingError(f"Ошибка обработки контакта {contact_id}")

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
        log = logger.bind(subdomain=settings.subdomain)
        group = group_data["group"]
        contact_ids = [c["id"] for c in group]
        try:
            main_contact, *duplicates = group
            payload = await prepare_merge_data(
                main_contact, duplicates, settings.priority_fields
            )
            log.debug(f"Payload для слияния: {payload}")

            merge_response = await self.amocrm_service.merge_contacts(
                settings.subdomain, access_token, payload
            )
            log.info(f"Слияние успешно для контактов: {contact_ids}")

            await self._add_merged_tag(
                settings.subdomain, access_token, main_contact["id"], payload
            )
            if matched_block_db_id := group_data.get("matched_block_db_id"):
                await self.duplicate_repo.insert_merge_block_log(
                    session, settings.subdomain, matched_block_db_id, main_contact["id"]
                )

            return merge_response
        except Exception as e:
            log.exception(f"Неизвестная ошибка при слиянии группы {contact_ids}: {e}")
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
