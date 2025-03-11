import asyncio
import aio_pika
import json
import uuid
from loguru import logger
from fastapi import HTTPException
import async_timeout

from src.rabbitmq.rmq_connetcion import RMQConnectionManager


class RPCClient:
    """Клиент для отправки RPC-запросов через RabbitMQ."""

    def __init__(self, connection_manager: RMQConnectionManager):
        self.connection_manager = connection_manager

    async def send_rpc_request_and_wait_for_reply(self, subdomain: str, client_id: str, timeout: int = 30):
        """Отправляет RPC-запрос и ожидает ответ."""

        correlation_id = str(uuid.uuid4())

        async with self.connection_manager as connection:
            channel = await connection.channel()
            reply_queue = await channel.declare_queue(exclusive=True)

            try:
                async with async_timeout.timeout(timeout):
                    message = aio_pika.Message(
                        body=json.dumps({"client_id": client_id, "subdomain": subdomain}).encode(),
                        correlation_id=correlation_id,
                        reply_to=reply_queue.name,
                    )

                    await channel.default_exchange.publish(message, routing_key="tokens_get_user")

                    async with reply_queue.iterator() as iter:
                        async for msg in iter:
                            async with msg.process():
                                if msg.correlation_id == correlation_id:
                                    return json.loads(msg.body.decode())

            except asyncio.TimeoutError:
                logger.error("RPC timeout")
                await reply_queue.delete()
                raise HTTPException(status_code=504, detail="Service timeout")

            finally:
                await reply_queue.delete()
