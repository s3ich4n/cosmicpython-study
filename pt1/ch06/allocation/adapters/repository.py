import abc
from typing import List

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from pt1.ch06.allocation.domain import model


class AbstractRepository(abc.ABC):

    @abc.abstractmethod
    async def add(self, batch: model.Batch):
        raise NotImplementedError

    @abc.abstractmethod
    async def get(self, reference) -> model.Batch:
        raise NotImplementedError


class FakeRepository(AbstractRepository):
    def __init__(self, batches):
        self._batches = set(batches)

    def add(self, batch):
        self._batches.add(batch)

    def get(self, reference):
        return next(b for b in self._batches if b.reference == reference)

    def list(self):
        return list(self._batches)


class SqlAlchemyRepository(AbstractRepository):
    def __init__(self, session: AsyncSession):
        self.session = session

    async def add(self, batch: model.Batch):
        """ Batch 객체를 Persistent store에 저장한다.

        sqlalchemy의 add를 호출해서 그런가?

        :param batch:
        :return:
        """
        self.session.add(batch)

    async def get(self, reference) -> model.Batch:
        return (
            (
                await self.session.execute(
                    select(model.Batch)
                    .options(selectinload(model.Batch.allocations))
                    .filter_by(reference=reference)
                )
            )
            .scalars()
            .one_or_none()
        )

    async def list(self) -> List[model.Batch]:
        return (
            (
                await self.session.scalars(
                    select(model.Batch)
                    .options(selectinload(model.Batch.allocations))
                )
            )
            .all()
        )

    async def delete_batch(self, batch: model.Batch):
        await self.session.delete(batch)
