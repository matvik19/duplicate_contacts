from dependency_injector import containers, providers

from src.common.config import CONNECTION_URL_RMQ, CONNECTION_URL_DB
from src.common.database import DatabaseManager
from src.common.token_service import TokenService
from src.duplicate_contact.repository import ContactDuplicateRepository
from src.duplicate_contact.services.duplicate_settings_service import (
    DuplicateSettingsService,
)
from src.rabbitmq.consumers.contact_duplicate_settings import (
    ContactDuplicateSettingsConsumer,
)
from src.rabbitmq.rmq_connetcion import RMQConnectionManager
from src.rabbitmq.rmq_manager import RMQManager
from src.rabbitmq.rmq_publisher import RMQPublisher
from src.rabbitmq.rpc_client import RPCClient


class Container(containers.DeclarativeContainer):
    """DI-контейнер для зависимостей."""

    # База данных
    db_manager = providers.Singleton(DatabaseManager, connection_url=CONNECTION_URL_DB)

    # RabbitMQ
    connection_manager = providers.Singleton(
        RMQConnectionManager, connection_url=CONNECTION_URL_RMQ
    )
    rmq_publisher = providers.Singleton(
        RMQPublisher, connection_manager=connection_manager
    )
    rpc_client = providers.Singleton(RPCClient, connection_manager=connection_manager)

    token_service = providers.Singleton(TokenService, rpc_client=rpc_client)

    # Репозиторий: теперь создаётся без сессии (она будет передаваться в методы)
    duplicate_repo = providers.Factory(ContactDuplicateRepository)

    # Сервис дублей
    duplicate_settings_service = providers.Factory(
        DuplicateSettingsService, duplicate_repo=duplicate_repo
    )

    # Консьюмер для настроек дублей
    contact_duplicates_settings_consumer = providers.Singleton(
        ContactDuplicateSettingsConsumer,
        queue_name="duplicate_contacts_settings",
        connection_manager=connection_manager,
        rmq_publisher=rmq_publisher,
        db_manager=db_manager,
        duplicate_settings_service=duplicate_settings_service,
    )

    consumers = providers.List(
        contact_duplicates_settings_consumer,
    )

    # Менеджер RabbitMQ
    rabbitmq_manager = providers.Singleton(
        RMQManager,
        connection_manager=connection_manager,
        db_manager=db_manager,
        rmq_publisher=rmq_publisher,
        consumers=consumers,
    )
