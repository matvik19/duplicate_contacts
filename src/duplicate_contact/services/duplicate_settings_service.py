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
                blocks=[
                    {
                        "block_id": block.block_id,
                        "fields": [
                            {
                                "field_name": bf.field_name,
                                "exclusion_fields": (
                                    [{"value": ex.value} for ex in bf.exclusion_values]
                                    if bf.exclusion_values
                                    else []
                                ),
                            }
                            for bf in block.fields
                        ],
                    }
                    for block in settings.blocks
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

            # Вставляем блоки дублей с учётом нового поля block_id
            block_mapping = {}
            if data.blocks:
                block_mapping = await self.duplicate_repo.insert_blocks(
                    session, new_settings_id, data.blocks
                )

            # Обрабатываем каждый блок: вставляем поля и их исключения
            for block in data.blocks:
                db_block_id = block_mapping.get(block["block_id"])
                if not db_block_id:
                    logger.warning(f"Пропущен блок без db_block_id: {block}")
                    continue

                if "fields" in block and block["fields"]:
                    # Вставляем поля блока и получаем mapping: field_name -> id
                    field_mapping = await self.duplicate_repo.insert_block_fields(
                        session, db_block_id, block["fields"]
                    )
                    # Для каждого поля, если есть исключения – вставляем их
                    for field in block["fields"]:
                        if "exclusion_fields" in field and field["exclusion_fields"]:
                            db_field_id = field_mapping.get(field["field_name"])
                            if not db_field_id:
                                logger.warning(
                                    f"Не найдено поле для исключений: {field}"
                                )
                                continue
                            await self.duplicate_repo.insert_exclusion_values(
                                session,
                                db_field_id,
                                field["field_name"],
                                field["exclusion_fields"],
                            )
            await session.commit()
            return {"id": new_settings_id, "subdomain": data.subdomain}

        except Exception as e:
            await session.rollback()
            logger.error(f"Ошибка при добавлении настроек: {e}")
            raise RuntimeError(f"Ошибка при добавлении настроек: {str(e)}")
