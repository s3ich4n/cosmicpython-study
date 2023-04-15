from dependency_injector import containers, providers

from pt1.ch04.adapters.postgres import AsyncSQLAlchemy
from pt1.ch04.config import Settings


class Container(containers.DeclarativeContainer):
    config = providers.Configuration()
    config.from_pydantic(Settings())

    db = providers.Singleton(
        AsyncSQLAlchemy,
        db_uri=config.data.DB_URI,
    )
