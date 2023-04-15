import pytest

from pt1.ch04.domain import model
from pt1.ch04.service_layer import services
from pt1.ch04.adapters.repository import AbstractRepository


class FakeRepository(AbstractRepository):
    def __init__(self, batches):
        self._batches = set(batches)

    async def add(self, batch):
        self._batches.add(batch)

    async def get(self, reference):
        return next(b for b in self._batches if b.reference == reference)

    async def list(self):
        return list(self._batches)


class FakeSession:
    committed = False

    async def commit(self):
        self.committed = True


@pytest.mark.asyncio
async def test_returns_allocation():
    line = model.OrderLine("o1", "COMPLICATED-LAMP", 10)
    batch = model.Batch("b1", "COMPLICATED-LAMP", 100, eta=None)
    repo = FakeRepository([batch])

    result = await services.allocate(line, repo, FakeSession())
    assert result == "b1"


@pytest.mark.asyncio
async def test_error_for_invalid_sku():
    line = model.OrderLine("o1", "NONEXISTENTSKU", 10)
    batch = model.Batch("b1", "AREALSKU", 100, eta=None)
    repo = FakeRepository([batch])

    with pytest.raises(services.InvalidSku, match="Invalid sku NONEXISTENTSKU"):
        await services.allocate(line, repo, FakeSession())


@pytest.mark.asyncio
async def test_commits():
    line = model.OrderLine("o1", "OMNIOUS-MIRROR", 10)
    batch = model.Batch("b1", "OMNIOUS-MIRROR", 100, eta=None)
    repo = FakeRepository([batch])
    session = FakeSession()

    await services.allocate(line, repo, session)
    assert session.committed is True


@pytest.mark.asyncio
async def test_deallocate():
    line = model.OrderLine("o1", "DEALLOC-TEST", 10)
    batch = model.Batch("b1", "DEALLOC-TEST", 100, eta=None)
    batch.allocate(line)    # 도메인 자체의 행동을 여기서 가정한다. 어차피 하는게 저거니까
    # 객체 간 데이터가 왔다갔다 하는 것을 영속화한다고 생각하라
    repo = FakeRepository([batch])

    assert batch.allocated_quantity == 10
    await services.deallocate(line, repo, FakeSession())

    assert batch.allocated_quantity == 0
