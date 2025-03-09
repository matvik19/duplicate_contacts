from src.common.config import CLIENT_ID
from loguru import logger
from fastapi import HTTPException


async def get_tokens_from_service(subdomain: str) -> str:
    """Получение токенов через RabbitMQ."""
    try:
        # Отправляем RPC запрос и ждем ответа
        tokens = await send_rpc_request_and_wait_for_reply(
            subdomain=subdomain, client_id=CLIENT_ID
        )
        if not tokens["access_token"] or not tokens["refresh_token"]:
            logger.error(f"Invalid tokens received: {tokens}")
            raise HTTPException(status_code=500, detail="Invalid tokens received")
        return tokens["access_token"]

    except Exception as e:
        logger.exception(f"Error during token retrieval: {e}")
        raise HTTPException(status_code=500, detail="Error during token retrieval")
