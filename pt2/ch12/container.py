from dependency_injector import containers, providers

from pt2.ch12.config import Settings
from pt2.ch12.src.allocation.adapters import redis
from pt2.ch12.src.allocation.adapters.postgres import AsyncSQLAlchemy
from pt2.ch12.src.allocation.service_layer import unit_of_work


class Container(containers.DeclarativeContainer):
    __self__ = providers.Self()

    config = providers.Configuration()
    config.from_pydantic(Settings())

    wiring_config = containers.WiringConfiguration(
        packages=[
            "pt2.ch12.src.allocation.entrypoints",
        ]
    )

    db = providers.Singleton(
        AsyncSQLAlchemy,
        db_uri=config.data.DB_URI,
    )

    redis_pool = providers.Resource(
        redis.init_redis_pool,
        redis_uri=config.broker.REDIS_URI,
    )

    redis = providers.Factory(
        redis.AsyncRedis,
        session=redis_pool,
    )

    allocation_uow = providers.Factory(
        unit_of_work.SqlAlchemyUnitOfWork,
        session_factory=db.provided.session_factory,
    )
