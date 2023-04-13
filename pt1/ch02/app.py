from asyncio import current_task

from typing import AsyncIterator

from fastapi import FastAPI, Depends
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    AsyncSession,
    async_scoped_session,
)
from sqlalchemy.orm import sessionmaker

from model import OrderLine, allocate
from orm import start_mappers, metadata
from repository import SqlAlchemyRepository


app = FastAPI()

async_engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=True)


async def init_connection():
    start_mappers()
    async with async_engine.begin() as conn:
        await conn.run_sync(metadata.create_all)


_async_session = AsyncSession(async_engine, expire_on_commit=False)
_async_session_maker = async_scoped_session(
    sessionmaker(
        async_engine,
        expire_on_commit=False,
        class_=AsyncSession,
    ),
    scopefunc=current_task,
)


@app.on_event("startup")
async def on_startup():
    await init_connection()


@app.on_event("shutdown")
async def on_shutdown():
    async with async_engine.begin() as conn:
        await conn.run_sync(metadata.drop_all)


async def init_session() -> AsyncIterator[AsyncSession]:
    async with _async_session_maker() as session:
        yield session


@app.post(
    "/allocate",
    status_code=201,
)
async def allocate_endpoint(
        order_line: OrderLine,
        session: AsyncSession = Depends(init_session),
):
    batches = await SqlAlchemyRepository(session).list()

    # call our domain service
    allocate(order_line, batches)

    await session.commit()
