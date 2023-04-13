import asyncio
from typing import Generator

import pytest_asyncio
from asgi_lifespan import LifespanManager
from httpx import AsyncClient
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from app import app
from orm import metadata, start_mappers


#
# FYI
#   https://pytest-asyncio.readthedocs.io/en/latest/reference/fixtures.html#fixtures
#
@pytest_asyncio.fixture(scope="session")
def event_loop(request) -> Generator:  # noqa: indirect usage
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="session", name="client")
async def test_client() -> AsyncClient:
    async with LifespanManager(app):
        async with AsyncClient(
            app=app,
            base_url='http://localhost:13370/',
        ) as client:
            yield client


@pytest_asyncio.fixture(scope="session", name="async_engine")
def async_engine_maker():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=True)
    # start_mappers()
    yield engine
    engine.sync_engine.dispose()


# @pytest_asyncio.fixture(name="create")
# async def make_metadata_to_db(async_engine):
#     async with async_engine.begin() as conn:
#         await conn.run_sync(metadata.create_all)
#     yield
#     async with async_engine.begin() as conn:
#         await conn.run_sync(metadata.drop_all)


@pytest_asyncio.fixture(name="session")
async def get_async_session(
        async_engine,
        # create,
):
    _async_session = AsyncSession(async_engine, expire_on_commit=False)
    _async_session_maker = sessionmaker(
        async_engine,
        expire_on_commit=False,
        class_=AsyncSession,
    )

    async with _async_session_maker() as _session:
        yield _session
