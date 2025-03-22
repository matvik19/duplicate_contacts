# contact_duplicate_repository.py

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy import select, insert, delete
from src.duplicate_contact.models import (
    Settings,
    PriorityField,
    Block,
    BlockField,
    ExclusionField,
)
from src.duplicate_contact.schemas import ContactDuplicateSettingsSchema


class ContactDuplicateRepository:
    """Репозиторий для работы с настройками дублей контактов."""

    settings: type[Settings] = Settings
    priority_fields: type[PriorityField] = PriorityField
    block: type[Block] = Block
    block_field: type[BlockField] = BlockField
    exclusion_fields: type[ExclusionField] = ExclusionField

    async def get_settings_by_subdomain(
        self, session: AsyncSession, subdomain: str
    ) -> Settings | None:
        """Получает `Settings` с полными связями по subdomain."""
        stmt = (
            select(self.settings)
            .where(self.settings.subdomain == subdomain)
            .options(
                selectinload(self.settings.priority_fields),
                selectinload(self.settings.blocks)
                .selectinload(self.block.fields)
                .selectinload(self.block_field.exclusion_values),
            )
        )
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    async def delete_settings_by_subdomain(
        self, session: AsyncSession, subdomain: str
    ) -> None:
        """Удаляет настройки по subdomain. Каскадное удаление всех зависимостей."""
        await session.execute(
            delete(self.settings).where(self.settings.subdomain == subdomain)
        )

    async def insert_settings(
        self, session: AsyncSession, data: ContactDuplicateSettingsSchema
    ) -> int:
        """Вставляет новую запись в таблицу `settings`."""
        stmt = (
            insert(self.settings)
            .values(
                subdomain=data.subdomain,
                merge_all=data.merge_all,
                blocked_creation=data.blocked_creation,
                merge_is_active=data.merge_is_active,
            )
            .returning(self.settings.id)
        )
        result = await session.execute(stmt)
        return result.scalar_one()

    async def insert_priority_fields(
        self, session: AsyncSession, settings_id: int, fields: list[dict]
    ) -> None:
        """Вставляет записи в `priority_fields`."""
        if not fields:
            return

        stmt = insert(self.priority_fields).values(
            [
                {
                    "field_name": field["field_name"],
                    "action": field["action"],
                    "settings_id": settings_id,
                }
                for field in fields
            ]
        )
        await session.execute(stmt)

    async def insert_blocks(
        self, session: AsyncSession, settings_id: int, blocks: list[dict]
    ) -> dict[int, int]:
        """Вставляет записи в `blocks` с сохранением block_id из клиента и возвращает соответствие: client block_id → db id."""
        if not blocks:
            return {}

        stmt = (
            insert(self.block)
            .values(
                [
                    {"settings_id": settings_id, "block_id": block["block_id"]}
                    for block in blocks
                ]
            )
            .returning(self.block.id, self.block.block_id)
        )
        result = await session.execute(stmt)
        rows = result.all()
        return {row.block_id: row.id for row in rows}

    async def insert_block_fields(
        self, session: AsyncSession, block_id: int, fields: list[dict]
    ) -> dict[str, int]:
        """Вставляет записи в `block_fields` и возвращает сопоставление: field_name → db id."""
        if not fields:
            return {}

        stmt = (
            insert(self.block_field)
            .values(
                [
                    {"field_name": field["field_name"], "block_id": block_id}
                    for field in fields
                ]
            )
            .returning(self.block_field.id, self.block_field.field_name)
        )
        result = await session.execute(stmt)
        rows = result.all()
        return {row.field_name: row.id for row in rows}

    async def insert_exclusion_values(
        self,
        session: AsyncSession,
        block_field_id: int,
        field_name: str,
        exclusion_fields: list[dict],
    ) -> None:
        """Вставляет записи в `exclusion_fields` для конкретного поля блока."""
        if not exclusion_fields:
            return

        stmt = insert(self.exclusion_fields).values(
            [
                {
                    "value": ex["value"],
                    "field_name": field_name,
                    "block_field_id": block_field_id,
                }
                for ex in exclusion_fields
            ]
        )
        await session.execute(stmt)
