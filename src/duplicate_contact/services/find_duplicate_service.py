from collections import defaultdict

from src.amocrm.service import AmocrmService


class FindDuplicateService:
    def __init__(self, amocrm_service: AmocrmService):
        self.amocrm_service = amocrm_service

    async def find_duplicates(
        self, subdomain, access_token, merge_fields: list
    ) -> list:
        """Ищет дубли контактов на основе указанных полей"""

        contacts = await self.amocrm_service.get_all_contacts(subdomain, access_token)
        duplicates = await self._group_duplicates(contacts, merge_fields)

        return duplicates

    async def _group_duplicates(
        self, contacts: list[dict], merge_fields: list[str]
    ) -> list:
        """
        Группирует контакты по совпадающим значениям в указанных полях.

        :param contacts: Список контактов.
        :param merge_fields: Поля для сравнения.
        :return: Список групп дублей.
        """
        grouped_contacts = defaultdict(list)

        for contact in contacts:
            keys = set()
            for field in merge_fields:
                print(field)
                field_value = await self._extract_field_value(contact, field)
                if field_value:
                    keys.add(field_value)

            for key in keys:
                grouped_contacts[key].append(contact)

        return [group for group in grouped_contacts.values() if len(group) > 1]

    @staticmethod
    async def _extract_field_value(contact: dict, field: str) -> str | None:
        """
        Извлекает значение указанного поля из контакта.

        :param contact: Данные контакта.
        :param field: Поле, которое нужно получить (например, "EMAIL" или "PHONE").
        :return: Значение поля или None.
        """
        custom_fields = contact.get("custom_fields_values")
        if not custom_fields:
            print(
                f"Поля custom_fields_values отсутствуют для контакта ID {contact['id']}"
            )
            return None

        # Проверяем custom_fields_values
        for field_data in custom_fields:
            if field_data.get("field_name") == field and "values" in field_data:
                return field_data["values"][0][
                    "value"
                ]  # Берём первое найденное значение

        # Если в кастомных нет, проверяем стандартные поля
        standard_value = contact.get(field)
        if standard_value:
            return standard_value

        print(f"Не найдено значение для {field} в контакте ID {contact['id']}")
        return None
