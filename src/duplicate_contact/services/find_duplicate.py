import time
from collections import defaultdict
from loguru import logger
from src.amocrm.service import AmocrmService


class DuplicateFinderService:
    """Сервис для поиска дублей контактов."""

    DAY_SECONDS = 86400

    def __init__(self, amocrm_service: AmocrmService):
        self.amocrm_service = amocrm_service

    async def find_duplicates_single_contact(
        self,
        subdomain: str,
        access_token: str,
        contact_id: int,
        blocks: list[dict],
        merge_all: bool = True,
    ) -> dict[str, any] | None:
        """Находит дубли для одного контакта."""
        main_contact = await self.amocrm_service.get_contact_by_id(
            subdomain, access_token, contact_id
        )
        if not main_contact or (not merge_all and not self._is_recent(main_contact)):
            logger.info(f"Контакт {contact_id} не найден или старше 24 часов.")
            return None

        candidates = await self._get_candidates(
            subdomain, access_token, contact_id, merge_all
        )
        return await self._find_matching_group(main_contact, candidates, blocks)

    async def find_duplicates_all_contacts(
        self,
        subdomain: str,
        access_token: str,
        blocks: list[dict],
        merge_all: bool = True,
    ) -> list[dict[str, any]]:
        """Находит все группы дублей."""
        contacts = await self.amocrm_service.get_all_contacts(subdomain, access_token)
        if not contacts:
            logger.info("Контакты не найдены.")
            return []

        if not merge_all:
            contacts = [c for c in contacts if self._is_recent(c)]

        return [
            {
                "group": sorted(group, key=lambda x: x.get("created_at", float("inf"))),
                "matched_block_db_id": block["db_id"],
            }
            for block in blocks
            for group in self._group_by_block(contacts, block)
        ]

    async def _get_candidates(
        self, subdomain: str, access_token: str, contact_id: int, merge_all: bool
    ) -> list[dict]:
        """Получает список кандидатов на дубли."""
        contacts = await self.amocrm_service.get_all_contacts(subdomain, access_token)
        candidates = [c for c in contacts if c["id"] != contact_id]
        return (
            candidates if merge_all else [c for c in candidates if self._is_recent(c)]
        )

    async def _find_matching_group(
        self, main_contact: dict, candidates: list[dict], blocks: list[dict]
    ) -> dict | None:
        """Ищет первую подходящую группу дублей."""
        for block in blocks:
            fields, exclusions = self._parse_block(block)
            if not fields:
                continue

            main_values = self._extract_values(main_contact, fields)
            if not main_values:
                continue

            duplicates = [
                c
                for c in candidates
                if self._is_duplicate(c, main_values, fields, exclusions)
            ]
            if duplicates:
                group = {
                    main_contact["id"]: main_contact,
                    **{c["id"]: c for c in duplicates},
                }
                return {
                    "group": sorted(
                        group.values(), key=lambda x: x.get("created_at", float("inf"))
                    ),
                    "matched_block_db_id": block.get("db_id"),
                }
        return None

    def _group_by_block(self, contacts: list[dict], block: dict) -> list[list[dict]]:
        """Группирует контакты по блоку."""
        fields, exclusions = self._parse_block(block)
        if not fields:
            logger.debug(f"Блок без полей: {block}")
            return []

        groups_dict = defaultdict(list)
        for contact in contacts:
            values = self._extract_values(contact, fields)
            if values:
                groups_dict[tuple(values.values())].append(contact)

        return [
            [c for c in group if not self._has_exclusion(c, exclusions, fields)]
            for group in groups_dict.values()
            if len(group) > 1
        ]

    def _extract_values(self, contact: dict, fields: list[str]) -> dict[str, str]:
        """Извлекает значения полей из контакта."""
        values = {}
        for field in fields:
            value = self.extract_field_value_simple(contact, field)
            if not value:
                logger.debug(f"Поле '{field}' пустое у контакта {contact['id']}")
                return {}
            values[field] = value
        return values

    def _is_duplicate(
        self, candidate: dict, main_values: dict, fields: list[str], exclusions: dict
    ) -> bool:
        """Проверяет, является ли контакт дублем."""
        candidate_values = self._extract_values(candidate, fields)
        return (
            candidate_values
            and all(candidate_values[f] == main_values[f] for f in fields)
            and not self._has_exclusion(candidate, exclusions, fields)
        )

    @staticmethod
    def _parse_block(block: dict) -> tuple[list[str], dict[str, list[str]]]:
        """Разбирает блок на поля и исключения."""
        fields = [f["field_name"] for f in block.get("fields", [])]
        exclusions = {
            f["field_name"]: [ex["value"] for ex in f.get("exclusion_fields", [])]
            for f in block.get("fields", [])
            if f.get("exclusion_fields")
        }
        return fields, exclusions

    @staticmethod
    def extract_field_value_simple(contact: dict, field_name: str) -> str | None:
        """Извлекает значение поля с нормализацией."""
        for field in contact.get("custom_fields_values", []):
            if field.get("field_name") == field_name and field.get("values"):
                value = field["values"][0].get("value")
                if value:
                    field_code = field.get("field_code", "").upper()
                    return (
                        DuplicateFinderService.normalize_phone(value)
                        if field_code == "PHONE"
                        else (
                            value
                            if field_code == "EMAIL"
                            else (
                                DuplicateFinderService.normalize_text(value)
                                if isinstance(value, str)
                                else value
                            )
                        )
                    )
        value = contact.get(field_name)
        return (
            DuplicateFinderService.normalize_text(value)
            if isinstance(value, str) and value
            else value
        )

    @staticmethod
    def _has_exclusion(contact: dict, exclusions: dict, fields: list[str]) -> bool:
        """Проверяет, все ли поля попадают в исключения."""
        if not exclusions:
            return False

        relevant_fields = [f for f in fields if f in exclusions]
        if not relevant_fields:
            return False

        return all(
            (value := DuplicateFinderService.extract_field_value_simple(contact, f))
            and value in exclusions[f]
            for f in relevant_fields
        )

    @staticmethod
    def _is_recent(contact: dict) -> bool:
        """Проверяет, создан ли контакт в последние 24 часа."""
        return (
            contact.get("created_at", 0)
            >= int(time.time()) - DuplicateFinderService.DAY_SECONDS
        )

    @staticmethod
    def normalize_text(text: str) -> str:
        return text.strip().lower()

    @staticmethod
    def normalize_phone(phone: str) -> str:
        digits = "".join(c for c in phone if c.isdigit())
        if len(digits) == 11 and digits.startswith("8"):
            digits = "7" + digits[1:]
        return digits
