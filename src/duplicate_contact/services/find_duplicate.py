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
    ) -> dict | None:
        """
        Находит дубли для конкретного контакта (contact_id) на основе настроек блоков.

        Склейка происходит только при полном совпадении всех полей блока.
        Если хотя бы одно поле пустое или значение отличается, переходим к следующему блоку.

        Возвращает словарь с ключами:
          - "group": список контактов (главный контакт + найденные дубликаты);
          - "matched_block_db_id": уникальный идентификатор блока (db_id), по которому найден дубль.
        """
        main_contact = await self.amocrm_service.get_contact_by_id(
            subdomain, access_token, contact_id
        )
        if not main_contact:
            logger.info(f"Контакт с id {contact_id} не найден.")
            return None

        # Если merge_all = False, проверяем, "новый" ли сам главный контакт
        if not merge_all:
            now_ts = int(time.time())
            day_ago_ts = now_ts - 86400  # 24 часа
            if main_contact.get("created_at", 0) < day_ago_ts:
                logger.info(
                    f"Главный контакт {contact_id} старше 24 часов, пропускаем слияние."
                )
                return None

        # Получаем кандидатов (все контакты, кроме главного)
        all_contacts = await self.amocrm_service.get_all_contacts(
            subdomain, access_token
        )
        candidates = [
            contact for contact in all_contacts if contact.get("id") != contact_id
        ]

        # Если merge_all = False, фильтруем кандидатов по дате создания
        if not merge_all:
            now_ts = int(time.time())
            day_ago_ts = now_ts - 86400
            candidates = [c for c in candidates if c.get("created_at", 0) >= day_ago_ts]

        # Проходим по каждому блоку
        for block in blocks:
            fields_for_merge, exclusion_mapping = self._parse_block(block)
            if not fields_for_merge:
                continue

            # Извлекаем значения главного контакта для всех полей блока
            main_values = {}
            for field_name in fields_for_merge:
                value = self.extract_field_value_simple(main_contact, field_name)
                # Если поле пустое, пропускаем блок
                if not value:
                    logger.debug(
                        f"Поле '{field_name}' пустое у главного контакта {contact_id}, пропускаем блок"
                    )
                    main_values = {}
                    break
                main_values[field_name] = value

            # Если не удалось извлечь все поля, переходим к следующему блоку
            if not main_values or len(main_values) != len(fields_for_merge):
                continue

            duplicates = []
            for candidate in candidates:
                candidate_values = {}
                # Проверяем все поля кандидата
                for field_name in fields_for_merge:
                    candidate_value = self.extract_field_value_simple(
                        candidate, field_name
                    )
                    # Если поле пустое или значение отличается, пропускаем кандидата
                    if not candidate_value:
                        logger.debug(
                            f"Поле '{field_name}' пустое у кандидата {candidate['id']}, пропускаем"
                        )
                        candidate_values = {}
                        break
                    if candidate_value != main_values[field_name]:
                        logger.debug(
                            f"Поле '{field_name}' отличается: '{candidate_value}' != '{main_values[field_name]}', пропускаем кандидата {candidate['id']}"
                        )
                        candidate_values = {}
                        break
                    candidate_values[field_name] = candidate_value

                # Если все поля совпали и не все значения в исключениях, добавляем в дубли
                if (
                    candidate_values
                    and len(candidate_values) == len(fields_for_merge)
                    and not self._has_exclusion(
                        candidate, exclusion_mapping, fields_for_merge
                    )
                ):
                    duplicates.append(candidate)

            # Если найдены дубли, формируем группу
            if duplicates:
                group = {main_contact["id"]: main_contact}
                for candidate in duplicates:
                    group[candidate["id"]] = candidate
                group_list = list(group.values())
                group_list.sort(key=lambda x: x.get("created_at", float("inf")))
                return {"group": group_list, "matched_block_db_id": block.get("db_id")}

        return None

    async def find_duplicates_all_contacts(
        self,
        subdomain: str,
        access_token: str,
        blocks: list[dict],
        merge_all: bool = True,
    ) -> list[dict]:
        """
        Находит группы дублей контактов на основе списка блоков.
        Склейка происходит только при полном совпадении всех полей блока.
        Исключения проверяются как "логическое И": все поля должны попадать в исключения, чтобы исключить склейку.
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
            for group in grouped:
                # Фильтруем группу по исключениям с учётом "логического И"
                filtered_group = [
                    contact
                    for contact in group
                    if not self._has_exclusion(
                        contact, exclusion_mapping, fields_for_merge
                    )
                ]
                if len(filtered_group) > 1:
                    sorted_group = sorted(
                        filtered_group, key=lambda x: x.get("created_at", float("inf"))
                    )
                    all_groups.append(
                        {
                            "group": sorted_group,
                            "matched_block_db_id": block.get("db_id"),
                        }
                    )

        return all_groups

    @staticmethod
    def _parse_block(block: dict) -> tuple[list[str], dict]:
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
            # Проверяем, что все поля блока присутствуют и имеют значения
            for field_name in fields_for_merge:
                value = self.extract_field_value_simple(contact, field_name)
                # Если поле пустое, пропускаем контакт
                if not value:
                    logger.debug(
                        f"Поле '{field_name}' пустое у контакта {contact['id']}, пропускаем"
                    )
                    key_values = []
                    break
                key_values.append(value)

            # Если не все поля заполнены, пропускаем контакт
            if not key_values or len(key_values) != len(fields_for_merge):
                continue

            key_tuple = tuple(key_values)
            groups_dict[key_tuple].append(contact)

        return [group for group in groups_dict.values() if len(group) > 1]

    def extract_field_value_simple(self, contact: dict, field_name: str) -> str | None:
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
                    return val
                else:
                    if isinstance(val, str):
                        return self.normalize_text(val)
                    return val
        standard_value = contact.get(field_name)
        if isinstance(standard_value, str) and standard_value:
            return self.normalize_text(standard_value)
        return standard_value if standard_value else None

    @staticmethod
    def _has_exclusion(
        contact: dict, exclusion_mapping: dict, fields_for_merge: list[str]
    ) -> bool:
        if not exclusion_mapping:
            return False

        fields_with_exclusions = [f for f in fields_for_merge if f in exclusion_mapping]
        if not fields_with_exclusions:
            return False

        all_excluded = True
        custom_fields = contact.get("custom_fields_values") or []

        for field_name in fields_with_exclusions:
            field_value = None
            for field_data in custom_fields:
                if field_data.get("field_name") == field_name:
                    value = field_data.get("values", [{}])[0].get("value")
                    if value:
                        field_code = (field_data.get("field_code") or "").upper()
                        if field_code == "PHONE":
                            field_value = FindDuplicateService.normalize_phone(value)
                        elif field_code == "EMAIL":
                            field_value = value
                        else:
                            field_value = (
                                FindDuplicateService.normalize_text(value)
                                if isinstance(value, str)
                                else value
                            )
                    break
            if not field_value:
                field_value = contact.get(field_name)
                if isinstance(field_value, str) and field_value:
                    field_value = FindDuplicateService.normalize_text(field_value)

            if not field_value or field_value not in exclusion_mapping[field_name]:
                all_excluded = False
                break

        if all_excluded:
            logger.debug(
                f"Все поля контакта попадают в исключения: {contact.get('id')}"
            )
        return all_excluded

    @staticmethod
    def normalize_text(text: str) -> str:
        return text.strip().lower()

    @staticmethod
    def normalize_phone(phone: str) -> str:
        digits = "".join(c for c in phone if c.isdigit())
        if len(digits) == 11 and digits.startswith("8"):
            digits = "7" + digits[1:]
        return digits
