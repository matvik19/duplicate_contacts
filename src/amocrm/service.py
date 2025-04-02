import asyncio

import aiohttp
from aiohttp import ClientSession
from loguru import logger
from fastapi import HTTPException
from typing import Dict, Any, List


class AmocrmService:
    """Сервис для работы с API amoCRM."""

    def __init__(self, client_session: ClientSession):
        self.client_session = client_session

    async def request(
        self, method: str, subdomain: str, access_token: str, endpoint: str, **kwargs
    ) -> Any:
        """Обобщённый метод для работы с API."""
        base_url = f"https://{subdomain}.amocrm.ru"
        url = f"{base_url}{endpoint}"
        headers = {
            "Host": f"{subdomain}.amocrm.ru",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {access_token}",
        }

        async with self.client_session.request(
            method, url, headers=headers, **kwargs
        ) as response:
            if response.status in [200, 201, 202]:
                return await response.json()
            elif response.status == 204:
                logger.warning(f"No content (204) for {url}.")
                return []
            else:
                error_message = await response.text()
                logger.error(f"Error {response.status} for {url}: {error_message}")
                raise HTTPException(status_code=response.status, detail=error_message)

    async def get_all_contacts(
        self, subdomain: str, access_token: str
    ) -> List[Dict[str, Any]]:
        """Получает все контакты асинхронно, отправляя несколько запросов сразу."""
        all_contacts = []
        limit = 250
        page = 1

        # Сначала запрашиваем первую страницу, чтобы узнать общее число контактов
        first_response = await self.request(
            "GET",
            subdomain,
            access_token,
            "/api/v4/contacts?with=leads",
            params={"page": page, "limit": limit},
        )
        contacts = first_response.get("_embedded", {}).get("contacts", [])
        all_contacts.extend(contacts)

        # Вычисляем, сколько всего страниц
        total_items = first_response.get("_total_items", 0)
        total_pages = (total_items // limit) + (1 if total_items % limit > 0 else 0)

        # Если контактов меньше 250, выходим
        if total_pages <= 1:
            return all_contacts

        # Запускаем параллельно запросы ко всем оставшимся страницам
        tasks = [
            self.request(
                "GET",
                subdomain,
                access_token,
                "/api/v4/contacts",
                params={"page": p, "limit": limit},
            )
            for p in range(2, total_pages + 1)
        ]
        responses = await asyncio.gather(*tasks)

        # Добавляем все полученные контакты
        for response in responses:
            contacts = response.get("_embedded", {}).get("contacts", [])
            all_contacts.extend(contacts)

        return all_contacts

    async def get_contact_by_id(
        self, subdomain: str, access_token: str, contact_id: int
    ) -> Dict[str, Any]:
        """Получает информацию о контакте по его ID."""
        return await self.request(
            "GET", subdomain, access_token, f"/api/v4/contacts/{contact_id}"
        )

    async def get_leads_by_filter(
        self,
        subdomain: str,
        access_token: str,
        pipeline_id: int,
        statuses_ids: List[int] = None,
        responsible_user_id: int = None,
    ) -> List[Dict[str, Any]]:
        """Получение сделок по фильтру."""
        params = {"filter[pipeline_id]": pipeline_id}
        if statuses_ids:
            for i, status_id in enumerate(statuses_ids):
                params[f"filter[status][{i}]"] = status_id
        if responsible_user_id:
            params["filter[responsible_user_id]"] = responsible_user_id

        return await self.request(
            "GET", subdomain, access_token, "/api/v4/leads?with=contacts", params=params
        )

    async def get_lead_by_id(
        self, subdomain: str, access_token: str, lead_id: int
    ) -> Dict[str, Any]:
        """Получение сделки по ID."""
        return await self.request(
            "GET", subdomain, access_token, f"/api/v4/leads/{lead_id}?with=contacts"
        )

    async def add_tag_to_lead(
        self,
        subdomain: str,
        access_token: str,
        lead_id: int,
        all_tags: List[Dict[str, Any]] = None,
    ) -> Dict:
        """Добавление тега 'merged' к сделке."""
        if all_tags:
            all_tags.append({"name": "merged"})
        else:
            all_tags = [{"name": "merged"}]

        payload = {
            "_embedded": {
                "tags": [
                    {"id": tag} if isinstance(tag, int) else tag for tag in all_tags
                ]
            }
        }
        return await self.request(
            "PATCH", subdomain, access_token, f"/api/v4/leads/{lead_id}", json=payload
        )

    async def merge_contacts(
        self,
        subdomain: str,
        access_token: str,
        result_element: dict,
    ) -> Dict[str, Any]:
        """
        Отправляет запрос на объединение контактов через API amoCRM.
        :param subdomain: поддомен amoCRM.
        :param access_token: access token для авторизации.
        :param result_element: тело запроса, сформированное методом prepare_merge_data.
        :return: ответ API в виде словаря.
        """
        url = f"https://{subdomain}.amocrm.ru/ajax/merge/contacts/save"
        headers = {
            "Host": f"{subdomain}.amocrm.ru",
            "Content-Type": "application/x-www-form-urlencoded",
            "X-Requested-With": "XMLHttpRequest",
            "Authorization": access_token,
        }

        try:
            async with self.client_session.post(
                url, data=result_element, headers=headers
            ) as response:
                if response.status != 202:
                    error_message = await response.text()
                    raise HTTPException(
                        status_code=response.status,
                        detail=f"Failed to merge contacts: {error_message}",
                    )
                result = await response.json()
                return result
        except aiohttp.ClientError as client_err:
            logger.error(f"Network error during merging contacts: {client_err}")
            raise HTTPException(
                status_code=502,
                detail=f"Network error during merging contacts: {client_err}",
            )
        except Exception as e:
            logger.error(f"Unexpected error during merging contacts: {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Unexpected error during merging contacts: {e}",
            )

    async def add_tag_merged_to_contact(
        self,
        subdomain: str,
        access_token: str,
        contact_id: int,
        all_tags: list | None,
    ) -> dict:
        """
        Добавляет тег "merged" к контакту в amoCRM.
        Если список тегов all_tags уже существует, дополняет его тегом "merged",
        иначе создает новый список с тегом "merged".
        """
        if all_tags:
            all_tags.append({"name": "merged"})
        else:
            all_tags = [{"name": "merged"}]

        payload = {
            "_embedded": {
                "tags": [
                    {"id": tag} if isinstance(tag, int) else tag for tag in all_tags
                ]
            }
        }

        endpoint = f"/api/v4/contacts/{contact_id}"
        return await self.request(
            "PATCH", subdomain, access_token, endpoint, json=payload
        )
