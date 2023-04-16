from datetime import datetime, timedelta

import pytest

from pt1.ch06.allocation.domain import model
from pt1.ch06.allocation.service_layer import services, unit_of_work
from pt1.ch06.allocation.adapters import repository
from pt1.ch06.tests.e2e.conftest import random_batchref

today = datetime.now()
tomorrow = today + timedelta(days=1)
later = tomorrow + timedelta(minutes=1)


class FakeUnitOfWork(unit_of_work.AbstractUnitOfWork):
    def __init__(self):
        self.batches = FakeRepository([])
        self.committed = False

    async def commit(self):
        self.committed = True

    async def rollback(self):
        pass


class FakeRepository(repository.AbstractRepository):
    def __init__(self, batches):
        self._batches = set(batches)

    async def add(self, batch):
        self._batches.add(batch)

    async def get(self, reference):
        return next(b for b in self._batches if b.reference == reference)

    async def list(self):
        return list(self._batches)

    @staticmethod
    def for_batch(ref, sku, qty, eta=None):
        return FakeRepository([
            model.Batch(ref, sku, qty, eta=None),
        ])

    @staticmethod
    def for_diff_batches(ref, sku, qty, eta_delta):
        batches = [
            model.Batch(ref[0], sku, qty, eta=today),
            model.Batch(ref[1], sku, qty, eta=today + timedelta(eta_delta)),
        ]
        return FakeRepository(batches)


class FakeSession:
    committed = False

    async def commit(self):
        self.committed = True


@pytest.mark.asyncio
async def test_add_batch():
    uow = FakeUnitOfWork()
    await services.add_batch("b1", "CRUNCHY-ARMCHAIR", 100, None, uow)

    assert await uow.batches.get("b1") is not None
    assert uow.committed


@pytest.mark.asyncio
async def test_returns_allocation():
    uow = FakeUnitOfWork()

    repo = FakeRepository.for_batch("b1", "COMPLICATED-LAMP", 100, eta=None)
    result = await services.allocate("o1", "COMPLICATED-LAMP", 10, uow)
    assert result == "b1"


@pytest.mark.asyncio
async def test_error_for_invalid_sku():
    repo = FakeRepository.for_batch("b1", "AREALSKU", 100, eta=None)

    with pytest.raises(services.InvalidSku, match="Invalid sku NONEXISTENTSKU"):
        await services.allocate("o1", "NONEXISTENTSKU", 10, repo, FakeSession())


@pytest.mark.asyncio
async def test_commits():
    repo = FakeRepository.for_batch("b1", "OMNIOUS-MIRROR", 100, eta=None)
    session = FakeSession()

    await services.allocate("o1", "OMNIOUS-MIRROR", 10, repo, session)
    assert session.committed is True


@pytest.mark.asyncio
async def test_deallocate():
    repo = FakeRepository.for_batch("o1", "DEALLOC-TEST", 10, eta=None)
    batch = await repo.get("o1")
    session = FakeSession()

    result = await services.allocate(
        orderid="o1",
        sku="DEALLOC-TEST",
        qty=10,
        repo=repo,
        session=session,
    )
    assert result == "o1"
    assert batch.allocated_quantity == 10

    await services.deallocate("o1", "DEALLOC-TEST", 10, repo, FakeSession())

    assert batch.allocated_quantity == 0
    assert session.committed


@pytest.mark.asyncio
async def test_prefers_current_stock_batches_to_shipments():
    in_stock_batch = random_batchref(1)
    shipment_batch = random_batchref(2)
    repo = FakeRepository.for_diff_batches(
        [in_stock_batch, shipment_batch],
        "RETRO-CLOCK",
        100,
        eta_delta=1,
    )

    await services.allocate("oref", "RETRO-CLOCK", 10, repo, FakeSession())

    in_stock_batch = await repo.get(in_stock_batch)
    assert in_stock_batch.available_quantity == 90

    shipment_batch = await repo.get(shipment_batch)
    assert shipment_batch.available_quantity == 100
