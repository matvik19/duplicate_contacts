from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession
from src.duplicate_contact.repository import ContactDuplicateRepository
from src.duplicate_contact.schemas import ContactDuplicateSettingsSchema


class DuplicateSettingsService:
    def __init__(self, duplicate_repo: ContactDuplicateRepository):
        self.duplicate_repo = duplicate_repo

    async def get_duplicate_settings(
        self, session: AsyncSession, subdomain: str
    ) -> ContactDuplicateSettingsSchema | None:
        """
        Получает настройки дублей контактов по subdomain.
        """
        try:
            settings = await self.duplicate_repo.get_settings_by_subdomain(
                session, subdomain
            )
            if not settings:
                return None

            return ContactDuplicateSettingsSchema(
                subdomain=settings.subdomain,
                merge_all=settings.merge_all,
                blocked_creation=settings.blocked_creation,
                duplicate_start=settings.duplicate_start,
                priority_fields=[
                    {"field_name": field.field_name, "action": field.action}
                    for field in settings.priority_fields
                ],
                duplicate_blocks=[
                    {
                        "id": block.id,
                        "fields": [
                            {"field_name": field.field_name} for field in block.fields
                        ],
                        "exclusion_fields": [
                            {"field_name": ex_field.field_name}
                            for ex_field in block.exclusion_fields
                        ],
                    }
                    for block in settings.duplicate_blocks
                ],
            )

        except Exception as e:
            logger.error(f"Ошибка при получении настроек дублей контактов: {e}")
            raise RuntimeError(f"Ошибка при получении настроек: {str(e)}")

    async def add_duplicate_settings(
        self, session: AsyncSession, data: ContactDuplicateSettingsSchema
    ) -> dict:
        """
        Добавляет настройки дублей. Если subdomain уже существует, удаляет старую запись и вставляет новую.
        """
        try:
            # Проверяем существующие настройки
            existing_settings = await self.duplicate_repo.get_settings_by_subdomain(
                session, data.subdomain
            )

            if existing_settings:
                await self.duplicate_repo.delete_settings_by_subdomain(
                    session, data.subdomain
                )

            # Вставляем основные настройки
            new_settings_id = await self.duplicate_repo.insert_settings(session, data)

            # Вставляем приоритетные поля
            if data.priority_fields:
                await self.duplicate_repo.insert_priority_fields(
                    session, new_settings_id, data.priority_fields
                )

            # Вставляем блоки дублей и получаем их реальные ID из БД
            block_mapping = {}
            if data.duplicate_blocks:
                block_mapping = await self.duplicate_repo.insert_blocks(
                    session, new_settings_id, data.duplicate_blocks
                )

            for index, block in enumerate(data.duplicate_blocks):
                block_id = block_mapping.get(
                    index
                )  # Используем индекс, а не `block.get("id")`

                if not block_id:
                    logger.warning(f"Пропущен блок без block_id: {block}")
                    continue

                # Вставляем поля блока
                if "fields" in block and block["fields"]:
                    await self.duplicate_repo.insert_block_fields(
                        session, block_id, block["fields"]
                    )

                # Вставляем исключённые поля
                if "exclusion_fields" in block and block["exclusion_fields"]:
                    await self.duplicate_repo.insert_exclusion_fields(
                        session, block_id, block["exclusion_fields"]
                    )
            await session.commit()
            return {"id": new_settings_id, "subdomain": data.subdomain}

        except Exception as e:
            await session.rollback()
            logger.error(f"Ошибка при добавлении настроек: {e}")
            raise RuntimeError(f"Ошибка при добавлении настроек: {str(e)}")
