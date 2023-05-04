import abc
from typing import List, Set

from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from pt2.ch08.src.allocation.domain import model


class AbstractRepository(abc.ABC):
    def __init__(self):
        self.seen: Set[model.Product] = set()

    async def add(self, product: model.Product):
        await self._add(product)
        self.seen.add(product)

    async def get(self, sku) -> model.Product:
        product = await self._get(sku)
        if product:
            self.seen.add(product)
        return product

    @abc.abstractmethod
    async def _add(self, product: model.Product):
        raise NotImplementedError

    @abc.abstractmethod
    async def _get(self, sku) -> model.Product:
        raise NotImplementedError


class SqlAlchemyRepository(AbstractRepository):
    def __init__(self, session: AsyncSession):
        super().__init__()
        self.session = session

    async def _add(self, product: model.Product):
        """ Batch 객체를 Persistent store에 저장한다.

        sqlalchemy의 add를 호출해서 그런가?

        """
        self.session.add(product)

    async def _get(self, sku: str) -> model.Product:
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
