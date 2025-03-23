import time
from collections import defaultdict
from loguru import logger
from src.amocrm.service import AmocrmService


class FindDuplicateService:
    def __init__(self, amocrm_service: AmocrmService) -> None:
        self.amocrm_service = amocrm_service

    async def find_duplicates_single_contact(
        self,
        subdomain: str,
        access_token: str,
        contact_id: int,
        blocks: list[dict],
        merge_all: bool = True,
    ) -> list[dict]:
        """
        Находит дубли для конкретного контакта (contact_id) на основе настроек блоков.

        Если merge_all=False, сливаем только те контакты, которые созданы в течение последних 24 часов.
        """
        main_contact = await self.amocrm_service.get_contact_by_id(
            subdomain, access_token, contact_id
        )
        if not main_contact:
            logger.info(f"Контакт с id {contact_id} не найден.")
            return []

        # Если merge_all = False, проверяем, "новый" ли сам главный контакт
        if not merge_all:
            now_ts = int(time.time())
            day_ago_ts = now_ts - 86400  # 24 часа
            if main_contact.get("created_at", 0) < day_ago_ts:
                logger.info(
                    f"Главный контакт {contact_id} старше 24 часов, пропускаем слияние."
                )
                return []

        # Получаем всех кандидатов
        all_contacts = await self.amocrm_service.get_all_contacts(
            subdomain, access_token
        )
        candidates = [
            contact for contact in all_contacts if contact.get("id") != contact_id
        ]

        # Если merge_all = False, фильтруем кандидатов по дате создания (только за последние 24 часа)
        if not merge_all:
            now_ts = int(time.time())
            day_ago_ts = now_ts - 86400
            candidates = [c for c in candidates if c.get("created_at", 0) >= day_ago_ts]

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
                if all(
                    self._extract_field_value_simple(candidate, field_name) == main_val
                    for field_name, main_val in main_values.items()
                ) and not self._has_exclusion(candidate, exclusion_mapping):
                    duplicates.append(candidate)

        if duplicates:
            # Собираем [main_contact, ...duplicates], убираем дубликаты по id
            group = {main_contact["id"]: main_contact}
            for candidate in duplicates:
                group[candidate["id"]] = candidate
            group_list = list(group.values())
            group_list.sort(key=lambda x: x.get("created_at", float("inf")))
            return group_list

        return []

    async def find_duplicates_all_contacts(
        self,
        subdomain: str,
        access_token: str,
        blocks: list[dict],
        merge_all: bool = True,
    ):
        """
        Находит группы дублей контактов на основе списка блоков (blocks).
        Если merge_all=False, то берём только контакты, созданные в течение последних 24 часов.
        """
        contacts = await self.amocrm_service.get_all_contacts(subdomain, access_token)
        if not contacts:
            logger.info("Контакты не найдены.")
            return []

        # Если merge_all=False, фильтруем контакты по дате создания
        if not merge_all:
            now_ts = int(time.time())
            day_ago_ts = now_ts - 86400
            contacts = [c for c in contacts if c.get("created_at", 0) >= day_ago_ts]

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
            skip_contact = False
            for field_name in fields_for_merge:
                value = self._extract_field_value_simple(contact, field_name)
                # Если значение поля отсутствует или пустое – пропускаем этот контакт.
                if not value:
                    skip_contact = True
                    break
                key_values.append(value)
            if skip_contact:
                continue
            key_tuple = tuple(key_values)
            groups_dict[key_tuple].append(contact)
        return [group for group in groups_dict.values() if len(group) > 1]

    def _extract_field_value_simple(self, contact: dict, field_name: str) -> str | None:
        custom_fields = contact.get("custom_fields_values") or []
        for field_data in custom_fields:
            if field_data.get("field_name") == field_name and "values" in field_data:
                val = field_data["values"][0].get("value")
                if not val:
                    continue
                field_code = (field_data.get("field_code") or "").upper()
                if field_code == "PHONE":
                    return self.normalize_phone(val)
                elif field_code == "EMAIL":
                    # Для EMAIL нормализацию текстового поля не применяем (или можно добавить .lower() при необходимости)
                    return val
                else:
                    # Для всех остальных текстовых полей приводим к единому виду
                    if isinstance(val, str):
                        return self.normalize_text(val)
                    return val
        # Если значение не найдено среди кастомных полей, пытаемся получить стандартное поле
        standard_value = contact.get(field_name)
        if isinstance(standard_value, str) and standard_value:
            # Если стандартное поле текстовое, то нормализуем его (так как это не EMAIL и не PHONE)
            return self.normalize_text(standard_value)
        return standard_value if standard_value else None

    @staticmethod
    def _has_exclusion(contact: dict, exclusion_mapping: dict) -> bool:
        custom_fields = contact.get("custom_fields_values") or []
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

    @staticmethod
    def normalize_text(text: str) -> str:
        """
        Приводит текст к нижнему регистру и обрезает пробелы по краям.
        """
        return text.strip().lower()

    @staticmethod
    def normalize_phone(phone: str) -> str:
        """
        Удаляет из номера телефона все символы, кроме цифр.
        Если номер состоит из 11 цифр и начинается с '8', заменяет её на '7'.
        """
        digits = "".join(c for c in phone if c.isdigit())
        if len(digits) == 11 and digits.startswith("8"):
            digits = "7" + digits[1:]
        return digits
