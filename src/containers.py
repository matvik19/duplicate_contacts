from dependency_injector import containers, providers
import aiohttp

from src.amocrm.service import AmocrmService
from src.common.config import CONNECTION_URL_DB, CONNECTION_URL_RMQ
from src.common.database import DatabaseManager
from src.common.token_service import TokenService
from src.duplicate_contact.repository import ContactDuplicateRepository
from src.duplicate_contact.services.contact_merge_service import ContactMergeService
from src.duplicate_contact.services.duplicate_settings import DuplicateSettingsService
from src.duplicate_contact.services.exclusion import ExclusionService
from src.duplicate_contact.services.find_duplicate import FindDuplicateService
from src.rabbitmq.consumers.add_exclusion import ExclusionConsumer
from src.rabbitmq.consumers.merge_all_contacts_consumer import MergeAllContactsConsumer
from src.rabbitmq.consumers.merge_single_contact_consumer import (
    MergeSingleContactConsumer,
)
from src.rabbitmq.consumers.save_settings import (
    ContactDuplicateSettingsConsumer,
)
from src.rabbitmq.connection import RMQConnectionManager
from src.rabbitmq.manager import RMQManager
from src.rabbitmq.publisher import RMQPublisher
from src.rabbitmq.rpc_client import RPCClient


class DatabaseContainer(containers.DeclarativeContainer):
    db_manager = providers.Singleton(DatabaseManager, connection_url=CONNECTION_URL_DB)


class RabbitMQContainer(containers.DeclarativeContainer):
    connection_manager = providers.Singleton(
        RMQConnectionManager, connection_url=CONNECTION_URL_RMQ
    )
    rmq_publisher = providers.Singleton(
        RMQPublisher, connection_manager=connection_manager
    )
    rpc_client = providers.Singleton(RPCClient, connection_manager=connection_manager)


class ServiceContainer(containers.DeclarativeContainer):
    client_session = providers.Singleton(
        lambda: aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False))
    )
    amocrm_service = providers.Factory(AmocrmService, client_session=client_session)
    token_service = providers.Singleton(
        TokenService, rpc_client=RabbitMQContainer.rpc_client
    )

    duplicate_repo = providers.Factory(ContactDuplicateRepository)
    duplicate_settings_service = providers.Factory(
        DuplicateSettingsService, duplicate_repo=duplicate_repo
    )
    find_duplicate_service = providers.Factory(
        FindDuplicateService, amocrm_service=amocrm_service
    )
    merge_contact_service = providers.Factory(
        ContactMergeService,
        find_duplicate_service=find_duplicate_service,
        amocrm_service=amocrm_service,
        duplicate_repo=duplicate_repo,
    )

    exclusion_service = providers.Factory(
        ExclusionService,
        amocrm_service=amocrm_service,
        duplicate_repo=duplicate_repo,
    )


class ConsumerContainer(containers.DeclarativeContainer):
    connection_manager = RabbitMQContainer.connection_manager
    rmq_publisher = RabbitMQContainer.rmq_publisher
    db_manager = DatabaseContainer.db_manager
    duplicate_settings_service = ServiceContainer.duplicate_settings_service
    token_service = ServiceContainer.token_service
    exclusion_service = ServiceContainer.exclusion_service

    save_contact_duplicates_settings_consumer = providers.Singleton(
        ContactDuplicateSettingsConsumer,
        queue_name="save_contact_duplicates_settings",
        connection_manager=connection_manager,
        rmq_publisher=rmq_publisher,
        db_manager=db_manager,
        duplicate_settings_service=duplicate_settings_service,
    )
    merge_duplicates_all_contacts_consumer = providers.Singleton(
        MergeAllContactsConsumer,
        queue_name="merge_duplicates_all_contacts",
        connection_manager=connection_manager,
        rmq_publisher=rmq_publisher,
        db_manager=db_manager,
        duplicate_service=ServiceContainer.merge_contact_service,
        token_service=token_service,
        duplicate_settings_service=duplicate_settings_service,
    )
    merge_duplicates_single_contact_consumer = providers.Singleton(
        MergeSingleContactConsumer,
        queue_name="merge_duplicates_single_contact",
        connection_manager=connection_manager,
        rmq_publisher=rmq_publisher,
        db_manager=db_manager,
        duplicate_service=ServiceContainer.merge_contact_service,
        token_service=token_service,
        duplicate_settings_service=duplicate_settings_service,
    )
    add_contact_in_exclusion_consumer = providers.Singleton(
        ExclusionConsumer,
        queue_name="add_contact_in_exclusion_consumer",
        connection_manager=connection_manager,
        rmq_publisher=rmq_publisher,
        db_manager=db_manager,
        exclusion_service=exclusion_service,
        token_service=token_service,
    )
    consumers = providers.List(
        save_contact_duplicates_settings_consumer,
        merge_duplicates_all_contacts_consumer,
        merge_duplicates_single_contact_consumer,
        add_contact_in_exclusion_consumer,
    )


class ApplicationContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    database = providers.Container(DatabaseContainer)
    rabbitmq = providers.Container(RabbitMQContainer)
    services = providers.Container(ServiceContainer)
    consumers = providers.Container(ConsumerContainer)

    rabbitmq_manager = providers.Singleton(
        RMQManager,
        connection_manager=rabbitmq.connection_manager,
        db_manager=database.db_manager,
        rmq_publisher=rabbitmq.rmq_publisher,
        consumers=consumers.consumers,
    )
