from loguru import logger
from src.amocrm.service import AmocrmService
from src.duplicate_contact.schemas import ContactDuplicateSettingsSchema
from src.duplicate_contact.services.find_duplicate import FindDuplicateService
from src.duplicate_contact.utils.prepare_merge_data import prepare_merge_data


class MergeAllContacts:
    def __init__(
        self,
        find_duplicate_service: FindDuplicateService,
        amocrm_service: AmocrmService,
    ):
        self.find_duplicate_service = find_duplicate_service
        self.amocrm_service = amocrm_service

    async def merge_duplicates(
        self,
        duplicate_settings: ContactDuplicateSettingsSchema,
        access_token: str,
    ) -> list[dict]:
        """
        Объединяет дублирующиеся контакты.
        """
        duplicate_groups = (
            await self.find_duplicate_service.find_duplicates_with_blocks(
                duplicate_settings.subdomain,
                access_token,
                duplicate_settings.blocks,
            )
        )
        logger.info(f"Найдено групп дублей: {duplicate_groups}")

        if not duplicate_groups:
            logger.info("Дубли не найдены.")
            return []

        for group in duplicate_groups:
            if len(group) < 2:
                continue
            main_contact = group[0]
            duplicates = group[1:]
            payload = await prepare_merge_data(
                main_contact, duplicates, duplicate_settings.priority_fields
            )
            logger.info(f"Подготовленный payload для слияния: {payload}")
            try:
                await self.amocrm_service.merge_contacts(
                    self.amocrm_service.client_session,
                    duplicate_settings.subdomain,
                    access_token,
                    payload,
                )
                logger.info(
                    f"Слияние успешно для контактов: {[c['id'] for c in group]}"
                )
            except Exception as e:
                logger.error(
                    f"Ошибка слияния для группы {[c['id'] for c in group]}: {e}"
                )
                continue
