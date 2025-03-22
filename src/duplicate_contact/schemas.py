from src.common.schema import BaseSchema


class ContactDuplicateSettingsSchema(BaseSchema):
    """Схема настроек дублей контактов."""

    subdomain: str
    merge_all: bool = True
    blocked_creation: bool = False
    merge_is_active: bool = False
    priority_fields: list = []
    blocks: list = []
