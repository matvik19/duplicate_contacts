import asyncio
from contextlib import asynccontextmanager
import uvicorn
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
from loguru import logger
from src.containers import ApplicationContainer


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом FastAPI."""
    # setup_logging()
    container = ApplicationContainer()
    db_manager = container.database.db_manager()
    rabbitmq_manager = container.rabbitmq_manager()

    await db_manager.wait_for_db()
    await db_manager.run_migrations()

    consumers_task = asyncio.create_task(rabbitmq_manager.start_all_consumers())

    logger.info("Все консьюмеры запущены.")

    try:
        yield
    finally:
        logger.info("Остановка консьюмеров...")
        consumers_task.cancel()
        await asyncio.gather(consumers_task, return_exceptions=True)

        logger.info("Закрытие соединения с БД...")
        await db_manager.close()

        logger.info("Закрытие соединения с RabbitMQ...")
        await rabbitmq_manager.connection_manager.close()

        logger.info("Все ресурсы успешно освобождены.")


app = FastAPI(
    title="Duplication contact widget",
    description="Microservice for handling contact duplication",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
    expose_headers=["X-Custom-Header"],
)


@app.post("/test_log")
async def test_log():
    logger.info("Test log message")
    return {"status": "logged"}


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        server_header=False,
        timeout_keep_alive=30,
    )
