from datetime import datetime, timedelta
from typing import List

import pytest

from pt2.ch09.src.allocation.domain import model, events
from pt2.ch09.src.allocation.service_layer import (
    unit_of_work,
    handlers,
    messagebus,
)
from pt2.ch09.src.allocation.adapters import repository
from pt2.ch09.tests.e2e.conftest import random_batchref


today = datetime.now()
tomorrow = today + timedelta(days=1)
later = tomorrow + timedelta(minutes=1)


class FakeRepository(repository.AbstractRepository):
    def __init__(
            self,
            products,
    ):
        super().__init__()
        self._products = set(products)

    async def add(self, products):
        self._products.add(products)

    async def get(self, sku) -> model.Product:
        return next((b for b in self._products if b.sku == sku), None)

    async def list(self) -> List[model.Product]:
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
        self.products = repository.TrackingRepository(FakeRepository([]))
        self.committed = False

    async def commit(self):
        self.committed = True

    async def rollback(self):
        pass


class TestAddBatch:
    @pytest.mark.asyncio
    async def test_add_batch(self):
        uow = FakeUnitOfWork()

        await messagebus.handle(
            events.BatchCreated("b1", "CRUNCHY-ARMCHAIR", 100, eta=None),
            uow,
        )

        assert await uow.products.get("CRUNCHY-ARMCHAIR") is not None
        assert uow.committed

    @pytest.mark.asyncio
    async def test_returns_allocation(self):
        uow = FakeUnitOfWork()

        await messagebus.handle(
            events.BatchCreated("b1", "COMPLICATED-LAMP", 100, eta=None),
            uow,
        )
        await messagebus.handle(
            events.BatchCreated("b2", "COMPLICATED-LAMP", 99, eta=None),
            uow,
        )

        assert "b2" in [
            b.reference for b in
            (await uow.products.get("COMPLICATED-LAMP")).batches
        ]


class TestAllocate:
    @pytest.mark.asyncio
    async def test_returns_allocation(self):
        uow = FakeUnitOfWork()
        await messagebus.handle(
            events.BatchCreated("batch1", "COMPLICATED-LAMP", 100, None),
            uow,
        )
        results = await messagebus.handle(
            events.AllocationRequired("o1", "COMPLICATED-LAMP", 10),
            uow,
        )

        assert results.popleft() == "batch1"

    @pytest.mark.asyncio
    async def test_error_for_invalid_sku(self):
        uow = FakeUnitOfWork()

        with pytest.raises(handlers.InvalidSku, match="Invalid sku NONEXISTENTSKU"):
            await messagebus.handle(
                events.BatchCreated("b1", "AREALSKU", 100, eta=None),
                uow,
            )
            await messagebus.handle(
                events.AllocationRequired("o1", "NONEXISTENTSKU", 10),
                uow,
            )

    @pytest.mark.asyncio
    async def test_commits(self):
        uow = FakeUnitOfWork()

        await messagebus.handle(
            events.BatchCreated("b1", "OMNIOUS-MIRROR", 100, eta=None),
            uow,
        )
        await messagebus.handle(
            events.AllocationRequired("o1", "OMNIOUS-MIRROR", 10),
            uow,
        )

        assert uow.committed is True

    @pytest.mark.asyncio
    async def test_prefers_current_stock_batches_to_shipments(self):
        uow = FakeUnitOfWork()
        in_stock_batch = random_batchref(1)
        shipment_batch = random_batchref(2)

        await messagebus.handle(
            events.BatchCreated(in_stock_batch, "RETRO-CLOCK", 100, eta=None),
            uow,
        )
        await messagebus.handle(
            events.BatchCreated(shipment_batch, "RETRO-CLOCK", 100, eta=None),
            uow,
        )
        await messagebus.handle(
            events.AllocationRequired("oref", "RETRO-CLOCK", 10),
            uow,
        )

        product = await uow.products.get("RETRO-CLOCK")
        in_stock_batch = product.get_allocation(in_stock_batch)
        assert in_stock_batch.available_quantity == 90

        shipment_batch = product.get_allocation(shipment_batch)
        assert shipment_batch.available_quantity == 100


class TestDeallocate:
    @pytest.mark.asyncio
    async def test_deallocate(self):
        uow = FakeUnitOfWork()

        await messagebus.handle(
            events.BatchCreated("o1", "DEALLOC-TEST", 100, eta=None),
            uow,
        )
        product = await uow.products.get("DEALLOC-TEST")
        batch = product.batches[0]

        result = await messagebus.handle(
            events.AllocationRequired("o1", "DEALLOC-TEST", 10),
            uow,
        )
        result = result.popleft()
        assert result == "o1"
        assert batch.allocated_quantity == 10

        await messagebus.handle(
            events.DeallocationRequired("o1", "DEALLOC-TEST", 10),
            uow,
        )

        assert batch.allocated_quantity == 0
        assert uow.committed
