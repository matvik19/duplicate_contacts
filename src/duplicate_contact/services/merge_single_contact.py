from loguru import logger
from src.amocrm.service import AmocrmService
from src.duplicate_contact.schemas import ContactDuplicateSettingsSchema
from src.duplicate_contact.services.find_duplicate import FindDuplicateService
from src.duplicate_contact.utils.prepare_merge_data import prepare_merge_data


class MergeOneContact:
    """
    Сервис для объединения дублей для конкретного контакта.

    Выполняет следующие шаги:
      1. Находит дубли для заданного контакта (по contact_id) с помощью метода find_duplicates_for_contact.
      2. Формирует тело запроса для объединения через prepare_merge_data.
      3. Вызывает метод merge_contacts у AmocrmService.
    """

    def __init__(
        self,
        find_duplicate_service: FindDuplicateService,
        amocrm_service: AmocrmService,
    ) -> None:
        self.find_duplicate_service = find_duplicate_service
        self.amocrm_service = amocrm_service

    async def merge_duplicate_for_contact(
        self,
        duplicate_settings: ContactDuplicateSettingsSchema,
        access_token: str,
        contact_id: int,
    ) -> dict:
        """
        Объединяет дубли для конкретного контакта (contact_id).

        :param duplicate_settings: Настройки дублей для данного subdomain.
        :param access_token: Токен доступа.
        :param contact_id: Идентификатор контакта, для которого нужно найти дубликаты.
        :return: Ответ API после слияния или пустой словарь, если дубликаты не найдены.
        """
        # Предполагаем, что метод find_duplicates_for_contact реализован в FindDuplicateService
        duplicate_group = await self.find_duplicate_service.find_duplicates_for_contact(
            duplicate_settings.subdomain,
            access_token,
            contact_id,
            duplicate_settings.blocks,
        )
        if not duplicate_group:
            logger.info(f"Дубликаты не найдены для контакта {contact_id}.")
            return {}

        logger.info(
            f"Найдена группа дублей для контакта {contact_id}: {duplicate_group}"
        )

        main_contact = duplicate_group[0]
        duplicates = duplicate_group[1:]
        payload = await prepare_merge_data(
            main_contact, duplicates, duplicate_settings.priority_fields
        )
        logger.info(
            f"Подготовленный payload для слияния контакта {contact_id}: {payload}"
        )

        try:
            response = await self.amocrm_service.merge_contacts(
                self.amocrm_service.client_session,
                duplicate_settings.subdomain,
                access_token,
                payload,
            )
            logger.info(f"Слияние успешно для контакта {contact_id}")
            return response
        except Exception as e:
            logger.error(f"Ошибка слияния для контакта {contact_id}: {e}")
            raise
