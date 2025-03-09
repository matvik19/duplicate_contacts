from loguru import logger
from elasticsearch import AsyncElasticsearch
import asyncio

ELASTICSEARCH_HOST = "http://91.197.98.62:9200"
es_client = AsyncElasticsearch([ELASTICSEARCH_HOST])


async def elasticsearch_handler(message):
    """
    Асинхронный обработчик для отправки логов в Elasticsearch.
    """
    log_record = message.record
    log_data = {
        "@timestamp": log_record["time"].isoformat(),
        "log.level": log_record["level"].name,
        "message": log_record["message"],
        "module": log_record["module"],
        "function": log_record["function"],
        "line": log_record["line"],
    }
    try:
        # Асинхронная отправка данных в Elasticsearch
        await es_client.index(index="allocations-logs", document=log_data)
    except Exception as e:
        print(f"Error sending log to Elasticsearch: {e}")


def setup_logging():
    """
    Установка асинхронного обработчика loguru для Elasticsearch.
    """
    logger.remove()

    logger.add(elasticsearch_handler, level="INFO", enqueue=True)
    logger.info("Logging setup complete.")
