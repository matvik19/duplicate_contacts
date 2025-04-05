from loguru import logger
from elasticsearch import AsyncElasticsearch
import asyncio
import os

ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "http://localhost:9200")
es_client = AsyncElasticsearch([ELASTICSEARCH_HOST])
LOG_INDEX = "amocrm-logs"


async def elasticsearch_handler(message):
    """
    Асинхронный обработчик для отправки логов в Elasticsearch.
    """
    record = message.record
    log_data = {
        "@timestamp": record["time"].isoformat(),
        "log.level": record["level"].name,
        "message": record["message"],
        "module": record["module"],
        "function": record["function"],
        "line": record["line"],
        "extra": record["extra"],  # Сохраняем дополнительные поля, если они есть
    }
    try:
        await es_client.index(index=LOG_INDEX, document=log_data)
    except Exception as e:
        print(f"[Log Error] Failed to send log to Elasticsearch: {e}")


def setup_logging():
    """
    Устанавливает loguru-логирование в Elasticsearch:
    - В Elasticsearch
    """
    logger.remove()  # Удаляем стандартный stdout

    logger.add(
        elasticsearch_handler,
        level="INFO",
        enqueue=True,
        backtrace=True,
        diagnose=True,
    )

    logger.info("✅ Логирование инициализировано.")
