from loguru import logger
from typing import List, Dict, Any

from sqlalchemy.ext.asyncio import AsyncSession

from src.amocrm.service import AmocrmService
from src.duplicate_contact.repository import ContactDuplicateRepository
from src.duplicate_contact.schemas import ContactDuplicateSettingsSchema
from src.duplicate_contact.services.find_duplicate import FindDuplicateService
from src.duplicate_contact.utils.prepare_merge_data import prepare_merge_data


class ContactMergeService:
    """
    Сервис склейки дублей контактов
      - Может сливать все дубли сразу.
      - Может сливать дубли для одного конкретного контакта.
    """

    def __init__(
        self,
        find_duplicate_service: FindDuplicateService,
        duplicate_repo: ContactDuplicateRepository,
        amocrm_service: AmocrmService,
    ):
        self.find_duplicate_service = find_duplicate_service
        self.duplicate_repo = duplicate_repo
        self.amocrm_service = amocrm_service

    async def merge_all_contacts(
        self,
        duplicate_settings: ContactDuplicateSettingsSchema,
        access_token: str,
        session: AsyncSession,
    ) -> List[Dict[str, Any]]:
        """
        Находит и объединяет все группы дублей контактов, соответствующие настройкам.
        Возвращает список результатов (можно адаптировать под нужды).
        """
        # Ищем все группы дублей
        duplicate_groups = (
            await self.find_duplicate_service.find_duplicates_all_contacts(
                subdomain=duplicate_settings.subdomain,
                access_token=access_token,
                blocks=duplicate_settings.blocks,
                merge_all=duplicate_settings.merge_all,
            )
        )
        logger.info(f"Найдено групп дублей: {duplicate_groups}")

        if not duplicate_groups:
            logger.info("Дубли не найдены.")
            return []

        results = []
        # Сливаем каждую группу
        for group in duplicate_groups:
            if len(group) < 2:
                continue
            merge_result = await self._merge_contact_group(
                group_data=group,
                duplicate_settings=duplicate_settings,
                access_token=access_token,
                session=session,
            )
            if merge_result is not None:
                results.append(merge_result)
        return results

    async def merge_single_contact(
        self,
        duplicate_settings: ContactDuplicateSettingsSchema,
        access_token: str,
        contact_id: int,
        session: AsyncSession,
    ) -> Dict[str, Any]:
        """
        Находит и объединяет дубли только для одного контакта (contact_id).
        Возвращает результат слияния или пустой словарь, если дублей нет.
        """
        # Ищем дубли для конкретного контакта
        duplicate_group = (
            await self.find_duplicate_service.find_duplicates_single_contact(
                subdomain=duplicate_settings.subdomain,
                access_token=access_token,
                contact_id=contact_id,
                blocks=duplicate_settings.blocks,
                merge_all=duplicate_settings.merge_all,
            )
        )
        if not duplicate_group or len(duplicate_group) < 2:
            logger.info(f"Дубликаты не найдены для контакта {contact_id}.")
            return {}

        # Сливаем найденную группу (тут она одна)
        merge_result = await self._merge_contact_group(
            group_data=duplicate_group,
            duplicate_settings=duplicate_settings,
            access_token=access_token,
            session=session,
        )
        return merge_result if merge_result else {}

    async def _merge_contact_group(
        self,
        group_data: dict,
        duplicate_settings: ContactDuplicateSettingsSchema,
        access_token: str,
        session: AsyncSession,
    ) -> Dict[str, Any] | None:
        """
        Вспомогательный метод, сливающий конкретную группу дублей (main_contact + duplicates).
        """
        group = group_data.get("group")
        matched_block_db_id = group_data.get("matched_block_db_id")
        if len(group) < 2:
            return None

        main_contact = group[0]
        duplicates = group[1:]

        # Формируем payload для слияния
        payload = await prepare_merge_data(
            main_contact, duplicates, duplicate_settings.priority_fields
        )
        logger.info(f"Подготовленный payload для слияния: {payload}")

        try:
            merge_response = await self.amocrm_service.merge_contacts(
                client_session=self.amocrm_service.client_session,
                subdomain=duplicate_settings.subdomain,
                access_token=access_token,
                result_element=payload,
            )
            logger.info(f"Слияние успешно для контактов: {[c['id'] for c in group]}")

            # 3. Добавляем тег "merged" (опционально)
            tags = payload.get("result_element[TAGS][]", [])
            await self.amocrm_service.add_tag_merged_to_contact(
                subdomain=duplicate_settings.subdomain,
                access_token=access_token,
                contact_id=main_contact["id"],
                all_tags=tags,
            )

            if matched_block_db_id:
                await self.duplicate_repo.insert_merge_block_log(
                    session,
                    duplicate_settings.subdomain,
                    matched_block_db_id,
                    main_contact["id"],
                )

            return merge_response
        except Exception as e:
            logger.error(f"Ошибка слияния для группы {[c['id'] for c in group]}: {e}")
            return None
