import asyncio

import aiohttp
from aiohttp import ClientSession
from loguru import logger

from src.common.exceptions import NetworkError, AmoCRMServiceError


class AmocrmService:
    """Сервис для работы с API amoCRM."""

    def __init__(self, client_session: ClientSession):
        self.client_session = client_session

    async def request(
        self, method: str, subdomain: str, access_token: str, endpoint: str, **kwargs
    ) -> any:
        log = logger.bind(subdomain=subdomain, endpoint=endpoint)
        base_url = f"https://{subdomain}.amocrm.ru"
        url = f"{base_url}{endpoint}"
        headers = {
            "Host": f"{subdomain}.amocrm.ru",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {access_token}",
        }

        try:
            async with self.client_session.request(
                method, url, headers=headers, **kwargs
            ) as response:
                if response.status in [200, 201, 202]:
                    log.debug(f"Успешный запрос: {method} {url}")
                    return await response.json()
                elif response.status == 204:
                    log.warning(f"Нет данных (204) для {url}")
                    return []
                else:
                    error_message = await response.text()
                    log.error(f"Ошибка {response.status} для {url}: {error_message}")
                    raise AmoCRMServiceError(
                        f"Ошибка API: {response.status} - {error_message}"
                    )
        except aiohttp.ClientError as e:
            log.error(f"Сетевая ошибка при запросе {url}: {e}")
            raise NetworkError(f"Сетевая ошибка: {e}")

    async def get_all_contacts(
        self, subdomain: str, access_token: str
    ) -> list[dict[str, any]]:
        log = logger.bind(subdomain=subdomain)
        all_contacts = []
        limit = 250
        page = 1

        first_response = await self.request(
            "GET",
            subdomain,
            access_token,
            "/api/v4/contacts?with=leads",
            params={"page": page, "limit": limit},
        )
        contacts = first_response.get("_embedded", {}).get("contacts", [])
        all_contacts.extend(contacts)

        total_items = first_response.get("_total_items", 0)
        total_pages = (total_items // limit) + (1 if total_items % limit > 0 else 0)

        if total_pages <= 1:
            log.info(f"Получено {len(all_contacts)} контактов на 1 странице")
            return all_contacts

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
        for response in responses:
            contacts = response.get("_embedded", {}).get("contacts", [])
            all_contacts.extend(contacts)

        log.info(f"Всего получено {len(all_contacts)} контактов")
        return all_contacts

    async def get_contact_by_id(
        self, subdomain: str, access_token: str, contact_id: int
    ) -> dict[str, any]:
        """Получает информацию о контакте по его ID."""
        return await self.request(
            "GET", subdomain, access_token, f"/api/v4/contacts/{contact_id}"
        )

    async def get_leads_by_filter(
        self,
        subdomain: str,
        access_token: str,
        pipeline_id: int,
        statuses_ids: list[int] = None,
        responsible_user_id: int = None,
    ) -> list[dict[str, any]]:
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
    ) -> dict[str, any]:
        """Получение сделки по ID."""
        return await self.request(
            "GET", subdomain, access_token, f"/api/v4/leads/{lead_id}?with=contacts"
        )

    async def add_tag_to_lead(
        self,
        subdomain: str,
        access_token: str,
        lead_id: int,
        all_tags: list[dict[str, any]] = None,
    ) -> dict:
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
        self, subdomain: str, access_token: str, result_element: dict
    ) -> dict[str, any]:
        log = logger.bind(subdomain=subdomain)
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
                    log.error(
                        f"Ошибка слияния контактов: {response.status} - {error_message}"
                    )
                    raise AmoCRMServiceError(f"Ошибка слияния: {error_message}")
                result = await response.json()
                log.info(f"Контакты успешно объединены: {result_element['id[]']}")
                return result
        except aiohttp.ClientError as e:
            log.error(f"Сетевая ошибка при слиянии: {e}")
            raise NetworkError(f"Сетевая ошибка: {e}")

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
