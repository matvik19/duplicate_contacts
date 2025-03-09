import aio_pika
from loguru import logger

class RMQPublisher:
    def __init__(self, connection_url):
        self.connection_url = connection_url

    async def send_response(self, message_body: str, reply_to: str, correlation_id: str):
        async with aio_pika.connect_robust(self.connection_url) as connection:
            channel = await connection.channel()
            message = aio_pika.Message(
                body=message_body.encode("utf-8"),
                correlation_id=correlation_id,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            )
            await channel.default_exchange.publish(message, routing_key=reply_to)
            logger.info(f"Ответ отправлен в {reply_to}")

    async def republish_message(self, message: aio_pika.IncomingMessage, new_retry_count: int):
        new_headers = dict(message.headers) if message.headers else {}
        new_headers["x-retry"] = new_retry_count

        new_message = aio_pika.Message(
            body=message.body,
            headers=new_headers,
            correlation_id=message.correlation_id,
            reply_to=message.reply_to,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )

        async with aio_pika.connect_robust(self.connection_url) as connection:
            channel = await connection.channel()
            await channel.default_exchange.publish(new_message, routing_key=message.routing_key)

        logger.info(f"Сообщение републиковалось с x-retry={new_retry_count}")
