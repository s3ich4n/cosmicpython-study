import abc
from typing import List

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

import model


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

        tips
            어차피 awaitable한 session이 들어가니까
            여기서 await할 필요는 없다? 흠ㅋㅋㅋㅋ

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
            .scalar_one()
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
