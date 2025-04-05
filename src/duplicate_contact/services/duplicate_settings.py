from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession

from src.common.exceptions import SettingsNotFoundError
from src.duplicate_contact.models import Settings
from src.duplicate_contact.repository import ContactDuplicateRepository
from src.duplicate_contact.schemas import ContactDuplicateSettingsSchema


class DuplicateSettingsService:
    """Сервис для управления настройками дублей."""

    def __init__(self, duplicate_repo: ContactDuplicateRepository):
        self.duplicate_repo = duplicate_repo

    async def get_duplicate_settings(
        self, session: AsyncSession, subdomain: str
    ) -> ContactDuplicateSettingsSchema | None:
        """Получает настройки дублей по subdomain."""
        try:
            settings = await self.duplicate_repo.get_settings_by_subdomain(
                session, subdomain
            )
            if not settings:
                raise SettingsNotFoundError(
                    f"Настройки не найдены для subdomain: {subdomain}"
                )
            return self._map_to_schema(settings)

        except Exception as e:
            logger.error(
                f"Ошибка получения настроек get_duplicate_settings для {subdomain}: {e}"
            )
            raise Exception(f"Ошибка при получении настроек: {str(e)}")

    async def add_duplicate_settings(
        self, session: AsyncSession, data: ContactDuplicateSettingsSchema
    ) -> dict[str, any]:
        """Добавляет или обновляет настройки дублей."""
        try:
            if await self.duplicate_repo.get_settings_by_subdomain(
                session, data.subdomain
            ):
                await self.duplicate_repo.delete_settings_by_subdomain(
                    session, data.subdomain
                )

            settings_id = await self._insert_settings(session, data)
            await session.commit()
            return {"id": settings_id, "subdomain": data.subdomain}
        except Exception as e:
            await session.rollback()
            logger.error(f"Ошибка добавления настроек для {data.subdomain}: {e}")
            raise Exception(f"Ошибка при добавлении настроек: {str(e)}")

    async def _insert_settings(
        self, session: AsyncSession, data: ContactDuplicateSettingsSchema
    ) -> int:
        """Вставляет настройки и связанные данные."""
        settings_id = await self.duplicate_repo.insert_settings(session, data)
        if data.priority_fields:
            await self.duplicate_repo.insert_priority_fields(
                session, settings_id, data.priority_fields
            )

        block_mapping = (
            await self.duplicate_repo.insert_blocks(session, settings_id, data.keys)
            if data.keys
            else {}
        )
        for block in data.keys or []:
            await self._insert_block_fields(
                session, block, block_mapping.get(block["block_id"])
            )
        return settings_id

    async def _insert_block_fields(
        self, session: AsyncSession, block: dict[str, any], db_block_id: int | None
    ) -> None:
        """Вставляет поля блока и их исключения."""
        if not db_block_id or "fields" not in block or not block["fields"]:
            logger.warning(f"Пропущен блок без db_block_id или полей: {block}")
            return

        field_mapping = await self.duplicate_repo.insert_block_fields(
            session, db_block_id, block["fields"]
        )
        for field in block["fields"]:
            if field.get("exclusion_fields"):
                db_field_id = field_mapping.get(field["field_name"])
                if db_field_id:
                    await self.duplicate_repo.insert_exclusion_values(
                        session,
                        db_field_id,
                        field["field_name"],
                        field["exclusion_fields"],
                    )
                else:
                    logger.warning(f"Не найдено поле для исключений: {field}")

    @staticmethod
    def _map_to_schema(settings: "Settings") -> ContactDuplicateSettingsSchema:
        """Преобразует объект Settings в схему."""
        return ContactDuplicateSettingsSchema(
            subdomain=settings.subdomain,
            merge_all=settings.merge_all,
            blocked_creation=settings.blocked_creation,
            merge_is_active=settings.merge_is_active,
            priority_fields=[f.field_name for f in settings.priority_fields],
            blocks=[
                {
                    "db_id": b.id,
                    "block_id": b.block_id,
                    "fields": [
                        {
                            "field_name": bf.field_name,
                            "exclusion_fields": [
                                {"value": ex.value} for ex in bf.exclusion_values or []
                            ],
                        }
                        for bf in b.fields
                    ],
                }
                for b in settings.keys
            ],
        )
