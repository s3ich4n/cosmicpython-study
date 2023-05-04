import abc

from sqlalchemy.ext.asyncio import AsyncSession

from pt2.ch08.src.allocation.adapters import repository
from pt2.ch08.src.allocation.service_layer import messagebus


class AbstractUnitOfWork(abc.ABC):
    products: repository.AbstractRepository

    async def __aenter__(self) -> 'AbstractUnitOfWork':
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.rollback()

    async def commit(self):
        await self._commit()
        await self.publish_event()

    async def publish_event(self):
        for product in self.products.seen:
            while product.events:
                event = product.events.pop(0)
                await messagebus.handle(event)

    @abc.abstractmethod
    async def _commit(self):
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
        self.session: AsyncSession = self._session_factory()
        self.products = repository.SqlAlchemyRepository(self.session)
        return await super().__aenter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await super().__aexit__(exc_type, exc_val, exc_tb)
        await self.session.close()

    async def _commit(self):
        await self.session.commit()

    async def rollback(self):
        await self.session.rollback()
