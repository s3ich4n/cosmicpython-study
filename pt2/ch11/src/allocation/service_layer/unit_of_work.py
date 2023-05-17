from __future__ import annotations

import abc
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession

from pt2.ch11.src.allocation.adapters import repository


class AbstractUnitOfWork(Protocol):
    products: repository.AbstractRepository

    async def __aenter__(self) -> AbstractUnitOfWork:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.rollback()

    @abc.abstractmethod
    async def commit(self):
        raise NotImplementedError

    @abc.abstractmethod
    async def rollback(self):
        raise NotImplementedError

    def collect_new_events(self):
        for product in self.products.seen:
            while product.messages:
                yield product.messages.pop(0)


class SqlAlchemyUnitOfWork(AbstractUnitOfWork):
    def __init__(
            self,
            session_factory,
    ):
        self._session_factory = session_factory

    async def __aenter__(self) -> AbstractUnitOfWork:
        self.session: AsyncSession = self._session_factory()
        self.products = repository.TrackingRepository(
            repository.SqlAlchemyRepository(self.session)
        )
        return await super().__aenter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await super().__aexit__(exc_type, exc_val, exc_tb)
        await self.session.close()

    async def commit(self):
        await self.session.commit()

    async def rollback(self):
        await self.session.rollback()
