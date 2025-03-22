from collections import defaultdict
from loguru import logger
from src.amocrm.service import AmocrmService


class FindDuplicateService:
    def __init__(self, amocrm_service: AmocrmService) -> None:
        self.amocrm_service = amocrm_service

    async def find_duplicates_for_contact(
        self,
        subdomain: str,
        access_token: str,
        contact_id: int,
        blocks: list[dict],
    ) -> list[dict]:
        """
        Находит дубли для конкретного контакта (contact_id) на основе настроек блоков.

        Алгоритм:
          1. Получаем основной контакт по contact_id.
          2. Получаем всех кандидатов (все контакты, кроме основного).
          3. Для каждого блока извлекаем поля для сравнения и словарь исключений.
          4. Из основного контакта собираем значения по указанным полям.
          5. Для каждого кандидата проверяем: если хотя бы по одному полю значение совпадает с основным
             и кандидат не содержит исключённых значений, то включаем его в дубли.
          6. Возвращаем группу, где первым будет основной контакт, а далее найденные дубли, отсортированные по created_at.

        :param subdomain: Поддомен amoCRM.
        :param access_token: Токен доступа.
        :param contact_id: Идентификатор основного контакта.
        :param blocks: Список блоков настроек дублей.
        :return: Группа контактов [main_contact, duplicate1, duplicate2, ...] или пустой список.
        """
        main_contact = await self.amocrm_service.get_contact_by_id(
            subdomain, access_token, contact_id
        )
        if not main_contact:
            logger.info(f"Контакт с id {contact_id} не найден.")
            return []

        all_contacts = await self.amocrm_service.get_all_contacts(
            subdomain, access_token
        )
        candidates = [
            contact for contact in all_contacts if contact.get("id") != contact_id
        ]

        duplicates = []
        for block in blocks:
            fields_for_merge, exclusion_mapping = self._parse_block(block)
            if not fields_for_merge:
                continue

            # Извлекаем значения из основного контакта для полей блока
            main_values = {}
            for field_name in fields_for_merge:
                value = self._extract_field_value_simple(main_contact, field_name)
                if value:
                    main_values[field_name] = value

            for candidate in candidates:
                for field_name, main_val in main_values.items():
                    candidate_val = self._extract_field_value_simple(
                        candidate, field_name
                    )
                    if candidate_val and candidate_val == main_val:
                        if not self._has_exclusion(candidate, exclusion_mapping):
                            duplicates.append(candidate)
                            break
        if duplicates:
            group = {main_contact["id"]: main_contact}
            for candidate in duplicates:
                group[candidate["id"]] = candidate
            group_list = list(group.values())
            group_list.sort(key=lambda x: x.get("created_at", float("inf")))
            return group_list
        return []

    async def find_duplicates_with_blocks(
        self,
        subdomain: str,
        access_token: str,
        blocks: list[dict],
    ):
        """
        Находит группы дублей контактов на основе списка блоков (blocks),
        учитывая поля и исключения, переданные внутри каждого блока.

        :param subdomain: Поддомен amoCRM.
        :param access_token: Токен доступа.
        :param blocks: Список блоков настроек дублей.
        :return: Список групп контактов-дублей.
        """
        contacts = await self.amocrm_service.get_all_contacts(subdomain, access_token)
        if not contacts:
            logger.info("Контакты не найдены.")
            return []

        all_groups = []
        for block in blocks:
            fields_for_merge, exclusion_mapping = self._parse_block(block)
            if not fields_for_merge:
                logger.debug(f"Блок без полей для сравнения: {block}")
                continue

            grouped = self._group_contacts_by_fields(contacts, fields_for_merge)
            filtered_groups = []
            for group in grouped:
                filtered_group = []
                for contact in group:
                    if self._has_exclusion(contact, exclusion_mapping):
                        continue
                    filtered_group.append(contact)
                if len(filtered_group) > 1:
                    sorted_group = sorted(
                        filtered_group, key=lambda x: x.get("created_at", float("inf"))
                    )
                    filtered_groups.append(sorted_group)
            all_groups.extend(filtered_groups)

        return all_groups

    @staticmethod
    def _parse_block(block: dict) -> tuple[list[str], dict]:
        """
        Извлекает из блока настройки:
          - Список полей для сравнения (fields_for_merge).
          - Словарь исключений: { field_name: [значение1, значение2, ...] }.

        :param block: Блок настроек дублей.
        :return: Кортеж (fields_for_merge, exclusion_mapping).
        """
        fields_for_merge = [f["field_name"] for f in block.get("fields", [])]
        exclusion_mapping = {}
        for field in block.get("fields", []):
            if field.get("exclusion_fields"):
                exclusion_mapping[field["field_name"]] = [
                    ex["value"] for ex in field["exclusion_fields"]
                ]
        return fields_for_merge, exclusion_mapping

    def _group_contacts_by_fields(
        self,
        contacts: list[dict],
        fields_for_merge: list[str],
    ) -> list[list[dict]]:
        groups_dict = defaultdict(list)
        for contact in contacts:
            key_values = []
            for field_name in fields_for_merge:
                value = self._extract_field_value_simple(contact, field_name)
                key_values.append(value)
            key_tuple = tuple(key_values)
            groups_dict[key_tuple].append(contact)
        return [group for group in groups_dict.values() if len(group) > 1]

    @staticmethod
    def _extract_field_value_simple(contact: dict, field_name: str) -> str | None:
        custom_fields = contact.get("custom_fields_values")
        if custom_fields:
            for field_data in custom_fields:
                if (
                    field_data.get("field_name") == field_name
                    and "values" in field_data
                ):
                    return field_data["values"][0].get("value")
        standard_value = contact.get(field_name)
        return standard_value if standard_value else None

    @staticmethod
    def _has_exclusion(contact: dict, exclusion_mapping: dict) -> bool:
        custom_fields = contact.get("custom_fields_values", [])
        for field_data in custom_fields:
            field_name = field_data.get("field_name")
            if field_name in exclusion_mapping:
                for val in field_data.get("values", []):
                    if val.get("value") in exclusion_mapping[field_name]:
                        logger.debug(
                            f"Исключение найдено для поля '{field_name}': значение '{val.get('value')}'"
                        )
                        return True
        for field_name, values in exclusion_mapping.items():
            if contact.get(field_name) in values:
                logger.debug(
                    f"Исключение найдено для стандартного поля '{field_name}': значение '{contact.get(field_name)}'"
                )
                return True
        return False
