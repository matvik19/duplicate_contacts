from src.common.config import CLIENT_ID
from loguru import logger
from fastapi import HTTPException

from src.common.exceptions import AmoCRMServiceError
from src.rabbitmq.rpc_client import RPCClient


class TokenService:
    """Класс для работы с токенами"""

    def __init__(self, rpc_client: RPCClient):
        self.rpc_client = rpc_client

    async def get_tokens(self, subdomain: str) -> str:
        """Запрашивает токен у другого сервиса через RPC"""
        tokens = await self.rpc_client.send_rpc_request_and_wait_for_reply(
            subdomain, CLIENT_ID
        )

        if not tokens.get("access_token") or not tokens.get("refresh_token"):
            raise AmoCRMServiceError("Invalid tokens received")

        return tokens["access_token"]
