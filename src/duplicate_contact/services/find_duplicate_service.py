from collections import defaultdict
from loguru import logger

from src.amocrm.service import AmocrmService


class FindDuplicateService:
    def __init__(self, amocrm_service: AmocrmService):
        self.amocrm_service = amocrm_service

    async def find_duplicates_with_blocks(
        self,
        subdomain: str,
        access_token: str,
        blocks: list[dict],
    ):
        """
        Находит группы дублей контактов на основе списка блоков (blocks),
        учитывая поля и исключения (exclusion_fields), которые теперь переданы внутри каждого поля.
        """
        contacts = await self.amocrm_service.get_all_contacts(subdomain, access_token)
        if not contacts:
            logger.info("Контакты не найдены.")
            return []

        all_groups = []
        for block in blocks:
            fields_for_merge = [f["field_name"] for f in block.get("fields", [])]
            # Формируем словарь исключений: field_name -> список исключаемых значений
            exclusion_mapping = {}
            for field in block.get("fields", []):
                if field.get("exclusion_fields"):
                    exclusion_mapping[field["field_name"]] = [
                        ex["value"] for ex in field["exclusion_fields"]
                    ]

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

    def _group_contacts_by_fields(
        self,
        contacts: list[dict],
        fields_for_merge: list[str],
    ):
        """
        Группирует контакты по совокупности полей (fields_for_merge).
        """
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
    def _extract_field_value_simple(contact: dict, field_name: str):
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
        """
        Проверяет, содержит ли контакт значение, которое есть в исключениях для соответствующего поля.
        """
        custom_fields = contact.get("custom_fields_values", [])
        for field_data in custom_fields:
            fname = field_data.get("field_name")
            if fname in exclusion_mapping:
                for val in field_data.get("values", []):
                    if val.get("value") in exclusion_mapping[fname]:
                        logger.debug(
                            f"Исключение найдено для поля '{fname}': значение '{val.get('value')}' присутствует в {exclusion_mapping[fname]}"
                        )
                        return True
        for fname, values in exclusion_mapping.items():
            if contact.get(fname) in values:
                logger.debug(
                    f"Исключение найдено для стандартного поля '{fname}': значение '{contact.get(fname)}' присутствует в {values}"
                )
                return True
        return False
