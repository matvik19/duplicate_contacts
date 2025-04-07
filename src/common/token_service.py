from src.common.config import CLIENT_ID
from loguru import logger
from fastapi import HTTPException

from src.common.exceptions import AmoCRMServiceError, TokenError
from src.rabbitmq.rpc_client import RPCClient


class TokenService:
    """Класс для работы с токенами"""

    def __init__(self, rpc_client: RPCClient):
        self.rpc_client = rpc_client

    async def get_tokens(self, subdomain: str) -> str:
        """Запрашивает токен у другого сервиса через RPC"""
        log = logger.bind(subdomain=subdomain)
        try:
            tokens = await self.rpc_client.send_rpc_request_and_wait_for_reply(
                subdomain, CLIENT_ID
            )
            if not tokens.get("access_token") or not tokens.get("refresh_token"):
                log.error("Получены некорректные токены: {}", tokens)
                raise TokenError("Invalid tokens received")

            return tokens["access_token"]
        except Exception as e:
            log.error("Ошибка RPC при запросе токена: {}", e)
            raise TokenError(f"Failed to fetch tokens: {e}")
