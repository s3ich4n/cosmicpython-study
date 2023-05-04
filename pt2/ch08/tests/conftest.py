import asyncio
from asyncio import current_task
from typing import Generator

import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from dependency_injector import providers
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_scoped_session, AsyncSession
from sqlalchemy.orm import sessionmaker

from pt2.ch08.src.allocation.adapters.orm import start_mappers
from pt2.ch08.src.allocation.adapters.postgres import AsyncSQLAlchemy
from pt2.ch08.container import Container
from pt2.ch08.src.allocation.entrypoints.app import app


@pytest_asyncio.fixture(scope="session")
def event_loop(request) -> Generator:  # noqa: indirect usage
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session", name="rdbms")
def make_container():
    container = Container()
    container.db.override(
        providers.Singleton(
            AsyncSQLAlchemy,
            db_uri="postgresql+asyncpg://test:test@localhost:5432/test",
        )
    )

    yield container.db()


@pytest_asyncio.fixture(scope="session", name="client")
async def test_client() -> AsyncClient:
    async with LifespanManager(app):
        async with AsyncClient(
            app=app,
            base_url='http://localhost:13370/',
        ) as client:
            yield client


@pytest_asyncio.fixture(scope="session", autouse=True)
def mapper():
    start_mappers()


@pytest_asyncio.fixture(scope="session", name="async_engine")
async def async_engine_maker(rdbms):
    await rdbms.connect(
        isolation_level="REPEATABLE READ",
        future=True,
        echo=True,
    )
    await rdbms.create_database()
    yield
    await rdbms.disconnect()


@pytest_asyncio.fixture(scope="session", name="async_session_maker")
async def get_async_session_maker(
        rdbms,
        async_engine,
):
    _async_session_maker = async_scoped_session(
        sessionmaker(
            rdbms.engine,
            expire_on_commit=False,
            class_=AsyncSession,
        ),
        scopefunc=current_task,
    )

    yield _async_session_maker


@pytest_asyncio.fixture(name="clear")
async def get_async_session(
        async_session_maker,
):
    async with async_session_maker() as _sess:
        yield
        await reset_table(_sess)


async def reset_table(session):
    await session.execute(text("DELETE FROM allocations"))
    await session.execute(text("DELETE FROM batches"))
    await session.execute(text("DELETE FROM products"))
    await session.execute(text("DELETE FROM order_lines"))
    await session.commit()
