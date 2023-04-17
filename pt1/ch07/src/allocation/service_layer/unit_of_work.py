import abc
from typing import List

from pt1.ch07.src.allocation.adapters import repository


class AbstractUnitOfWork(abc.ABC):
    products: repository.AbstractProductRepository

    async def __aenter__(self) -> 'AbstractUnitOfWork':
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.rollback()

    @abc.abstractmethod
    async def commit(self):
        raise NotImplementedError

    @abc.abstractmethod
    async def rollback(self):
        raise NotImplementedError


class SqlAlchemyUnitOfWork(AbstractUnitOfWork):
    def __init__(
            self,
            session_factory,
    ):
        self._session_factory = session_factory

    async def __aenter__(self):
        self.session = self._session_factory()  # type: AsyncSession
        self.batches = repository.SqlAlchemyRepository(self.session)
        return await super().__aenter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await super().__aexit__(exc_type, exc_val, exc_tb)
        await self.session.close()

    async def commit(self):
        await self.session.commit()

    async def rollback(self):
        await self.session.rollback()


class AbstractProductRepository(abc.ABC):
    @abc.abstractmethod
    async def get(self, reference) -> List['Product']:
        raise NotImplementedError

    @abc.abstractmethod
    async def add(self, product: 'Product'):
        raise NotImplementedError
