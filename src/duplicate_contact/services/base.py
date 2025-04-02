from abc import ABC
from typing import Optional, Dict, List, Any
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession
from src.amocrm.service import AmocrmService


class ContactService(ABC):
    """Базовый класс для сервисов, работающих с контактами."""

    def __init__(self, amocrm_service: AmocrmService):
        self.amocrm_service = amocrm_service

    async def get_contact(
        self, subdomain: str, access_token: str, contact_id: int
    ) -> dict[str, Any] | None:
        """Получает контакт по ID"""
        contact = await self.amocrm_service.get_contact_by_id(
            subdomain, access_token, contact_id
        )
        if not contact:
            logger.error(f"Контакт с id {contact_id} не найден в amoCRM")
        return contact
