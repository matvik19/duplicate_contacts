from src.common.schema import BaseSchema


class ContactDuplicateSettingsSchema(BaseSchema):
    """Схема настроек дублей контактов."""

    subdomain: str
    merge_all: bool = True
    blocked_creation: bool = False
    duplicate_start: bool = False
    priority_fields: list = []
    duplicate_blocks: list = []
