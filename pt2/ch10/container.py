from dependency_injector import containers, providers

from pt2.ch10.src.allocation.adapters.postgres import AsyncSQLAlchemy
from pt2.ch10.src.allocation.service_layer import unit_of_work
from pt2.ch10.config import Settings


class Container(containers.DeclarativeContainer):
    config = providers.Configuration()
    config.from_pydantic(Settings())

    db = providers.Singleton(
        AsyncSQLAlchemy,
        db_uri=config.data.DB_URI,
    )

    allocation_uow = providers.Factory(
        unit_of_work.SqlAlchemyUnitOfWork,
        session_factory=db.provided.session_factory,
    )
