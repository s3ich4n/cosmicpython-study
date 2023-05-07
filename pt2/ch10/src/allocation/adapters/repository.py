from typing import List, Set, Protocol

from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from pt2.ch10.src.allocation.adapters import orm
from pt2.ch10.src.allocation.domain import model


class AbstractRepository(Protocol):
    async def add(self, product: model.Product):
        raise NotImplementedError

    async def get(self, sku) -> model.Product:
        raise NotImplementedError

    async def get_by_batchref(self, batchref) -> model.Product:
        raise NotImplementedError

    async def list(self) -> List[model.Product]:
        raise NotImplementedError


class TrackingRepository:
    seen = Set[model.Product]

    def __init__(
            self,
            repo: AbstractRepository,
    ):
        self._repo = repo
        self.seen = set()

    async def add(self, product: model.Product):
        await self._repo.add(product)
        self.seen.add(product)

    async def get(self, sku) -> model.Product:
        product = await self._repo.get(sku)
        if product:
            self.seen.add(product)
        return product

    async def get_by_batchref(self, batchref) -> model.Product:
        product = await self._repo.get_by_batchref(batchref)
        if product:
            self.seen.add(product)
        return product

    async def list(self) -> List[model.Product]:
        return await self._repo.list()


class SqlAlchemyRepository(AbstractRepository):
    def __init__(
            self,
            session: AsyncSession,
    ):
        self.session = session

    async def add(self, product: model.Product):
        """ Batch 객체를 Persistent store에 저장한다.

        sqlalchemy의 add를 호출해서 그런가?

        """
        self.session.add(product)

    async def get(self, sku: str) -> model.Product:
        return (
            (
                await self.session.execute(
                    select(model.Product)
                    .options(selectinload(model.Product.batches))
                    .filter(model.Product.sku == sku)
                )
            )
            .scalars()
            .one_or_none()
        )

    async def get_by_batchref(self, batchref) -> model.Product:
        return (
            (
                await self.session.execute(
                    select(model.Product)
                    .join(model.Batch)
                    .filter(orm.batches.c.reference == batchref)
                )
            )
            .scalars()
            .one_or_none()
        )

    async def list(self) -> List[model.Product]:
        return (
            (
                await self.session.scalars(
                    select(model.Product)
                    .options(selectinload(model.Product.batches))
                )
            )
            .all()
        )

    async def delete_batch(self, batch: model.Batch):
        await self.session.execute(
            delete(model.Product)
            .filter(model.Product.sku == batch.sku)
        )
