from datetime import datetime, timedelta

import pytest

from pt1.ch07.src.allocation.domain import model
from pt1.ch07.src.allocation.service_layer import unit_of_work, services
from pt1.ch07.src.allocation.adapters import repository
from pt1.ch07.tests.e2e.conftest import random_batchref


today = datetime.now()
tomorrow = today + timedelta(days=1)
later = tomorrow + timedelta(minutes=1)


class FakeRepository(repository.AbstractRepository):
    def __init__(
            self,
            products,
    ):
        self._products = set(products)

    async def add(self, products):
        self._products.add(products)

    async def get(self, sku):
        return next((b for b in self._products if b.sku == sku), None)

    async def list(self):
        return list(self._products)

    @staticmethod
    def for_batch(ref, sku, qty, eta=None):
        batch = model.Batch(ref, sku, qty, eta=None)
        return FakeRepository(
            [model.Product(sku, [batch, ])]
        )

    @staticmethod
    def for_diff_batches(ref, sku, qty, eta_delta):
        batches = [
            model.Batch(ref[0], sku, qty, eta=today),
            model.Batch(ref[1], sku, qty, eta=today + timedelta(eta_delta)),
        ]
        return FakeRepository(
            [model.Product(sku, batches)]
        )


class FakeUnitOfWork(unit_of_work.AbstractUnitOfWork):
    def __init__(self):
        self.products = FakeRepository([])
        self.committed = False

    async def commit(self):
        self.committed = True

    async def rollback(self):
        pass


@pytest.mark.asyncio
async def test_add_batch():
    uow = FakeUnitOfWork()
    await services.add_batch("b1", "CRUNCHY-ARMCHAIR", 100, eta=None, uow=uow)

    assert await uow.products.get("CRUNCHY-ARMCHAIR") is not None
    assert uow.committed


@pytest.mark.asyncio
async def test_returns_allocation():
    uow = FakeUnitOfWork()
    await services.add_batch("b1", "COMPLICATED-LAMP", 100, eta=None, uow=uow)

    result = await services.allocate("o1", "COMPLICATED-LAMP", 10, uow)
    assert result == "b1"


@pytest.mark.asyncio
async def test_error_for_invalid_sku():
    uow = FakeUnitOfWork()
    await services.add_batch("b1", "AREALSKU", 100, eta=None, uow=uow)

    with pytest.raises(services.InvalidSku, match="Invalid sku NONEXISTENTSKU"):
        await services.allocate("o1", "NONEXISTENTSKU", 10, uow)


@pytest.mark.asyncio
async def test_commits():
    uow = FakeUnitOfWork()
    await services.add_batch("b1", "OMNIOUS-MIRROR", 100, eta=None, uow=uow)
    await services.allocate("o1", "OMNIOUS-MIRROR", 10, uow)

    assert uow.committed is True


@pytest.mark.asyncio
async def test_deallocate():
    uow = FakeUnitOfWork()

    await services.add_batch("o1", "DEALLOC-TEST", 10, eta=None, uow=uow)
    product = await uow.products.get("DEALLOC-TEST")
    batch = product.batches[0]

    result = await services.allocate(
        orderid="o1",
        sku="DEALLOC-TEST",
        qty=10,
        uow=uow,
    )
    assert result == "o1"
    assert batch.allocated_quantity == 10

    await services.deallocate("o1", "DEALLOC-TEST", 10, uow)

    assert batch.allocated_quantity == 0
    assert uow.committed


@pytest.mark.asyncio
async def test_prefers_current_stock_batches_to_shipments():
    uow = FakeUnitOfWork()
    in_stock_batch = random_batchref(1)
    shipment_batch = random_batchref(2)

    await services.add_batch(in_stock_batch, "RETRO-CLOCK", 100, eta=None, uow=uow)
    await services.add_batch(shipment_batch, "RETRO-CLOCK", 100, eta=None, uow=uow)
    await services.allocate("oref", "RETRO-CLOCK", 10, uow)

    product = await uow.products.get("RETRO-CLOCK")
    in_stock_batch = product.get_allocation(in_stock_batch)
    assert in_stock_batch.available_quantity == 90

    shipment_batch = product.get_allocation(shipment_batch)
    assert shipment_batch.available_quantity == 100
