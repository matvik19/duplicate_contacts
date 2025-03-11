from dependency_injector import containers, providers

from src.common.config import CONNECTION_URL_RMQ, CONNECTION_URL_DB
from src.common.database import DatabaseManager
from src.common.token_service import TokenService
from src.rabbitmq.consumers.contact_duplicate_consumer import ContactDuplicateConsumer
from src.rabbitmq.rmq_connetcion import RMQConnectionManager
from src.rabbitmq.rmq_manager import RMQManager
from src.rabbitmq.rmq_publisher import RMQPublisher
from src.rabbitmq.rpc_client import RPCClient


class Container(containers.DeclarativeContainer):
    # Можно передать конфиг
    # config = providers.Configuration()

    db_manager = providers.Singleton(DatabaseManager, database_url=CONNECTION_URL_DB)
    connection_manager = providers.Singleton(
        RMQConnectionManager, connection_url=CONNECTION_URL_RMQ
    )
    rmq_publisher = providers.Singleton(
        RMQPublisher, connection_manager=connection_manager
    )
    rpc_client = providers.Singleton(RPCClient, connection_manager=connection_manager)

    rabbitmq_manager = providers.Singleton(
        RMQManager,
        connection_manager=connection_manager,
        db_manager=db_manager,
        rmq_publisher=rmq_publisher,
    )

    token_service = providers.Singleton(TokenService, rpc_client=rpc_client)

    contact_duplicates_consumer = providers.Factory(
        ContactDuplicateConsumer,
        queue_name="duplicate_contacts",
        connection_manager=connection_manager,
        rmq_publisher=rmq_publisher,
        db_manager=db_manager,
    )
