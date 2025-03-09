import asyncio
import subprocess
from contextlib import asynccontextmanager
from loguru import logger

import asyncpg
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from src.common.config import DB_HOST, DB_NAME, DB_PASS, DB_PORT, DB_USER
Base = declarative_base()

class DatabaseManager:
    def __init__(self):
        """Инициализируем параметры подключения и движок SQLAlchemy."""
        self.database_url = f"postgresql+asyncpg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        self.engine = create_async_engine(
            self.database_url,
            pool_size=30,
            max_overflow=25,
            echo=False,
            pool_timeout=30,
            pool_recycle=1800,
            pool_pre_ping=True,
        )
        self.async_session_maker = sessionmaker(
            self.engine, class_=AsyncSession, expire_on_commit=False
        )

    @asynccontextmanager
    async def get_session(self) -> AsyncSession:
        """Возвращает сессию БД в виде асинхронного контекстного менеджера."""
        async with self.async_session_maker() as session:
            try:
                yield session
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()

    @staticmethod
    async def wait_for_db(retries: int = 10, delay: int = 2):
        """Ожидаем доступности базы данных перед запуском сервиса."""
        while retries:
            try:
                conn = await asyncpg.connect(
                    user=DB_USER,
                    password=DB_PASS,
                    database=DB_NAME,
                    host=DB_HOST,
                    port=DB_PORT,
                )
                await conn.close()
                logger.info("✅ Database is ready.")
                return
            except Exception as e:
                logger.warning(
                    f"Database not ready, retrying... {retries} attempts left. Error: {e}"
                )
                retries -= 1
                await asyncio.sleep(delay)
        logger.error("Database is not available, exiting.")
        exit(1)

    @staticmethod
    async def run_migrations():
        """Запускает Alembic миграции перед стартом сервиса."""
        logger.info("Running database migrations...")
        subprocess.run("alembic upgrade head", shell=True, check=True)
        logger.info("Migrations completed.")

    async def close(self):
        """Закрывает соединение с БД."""
        await self.engine.dispose()
        logger.info("Database connection closed.")
