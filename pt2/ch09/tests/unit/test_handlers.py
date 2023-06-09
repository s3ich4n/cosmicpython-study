from collections import deque
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

    async def get_by_batchref(self, batchref) -> model.Product:
        return next((
            p for p in self._products for b in p.batches
            if b.reference == batchref),
            None
        )

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


class FakeMessageBus(messagebus.AbstractMessageBus):
    def __init__(self):
        self.events_published: deque[events.Event] = deque()
        self.handlers = {
            events.BatchCreated: [lambda x: self.events_published.append(x)],
            events.OutOfStock: [lambda x: self.events_published.append(x)],
            events.AllocationRequired: [lambda x: self.events_published.append(x)],
            events.DeallocationRequired: [lambda x: self.events_published.append(x)],
            events.BatchQuantityChanged: [lambda x: self.events_published.append(x)],
        }


class FakeUnitOfWork(unit_of_work.AbstractUnitOfWork):
    def __init__(self):
        self.committed = False
        self.events_published: deque[events.Event] = deque()
        self.products = repository.TrackingRepository(FakeRepository([]))

    async def commit(self):
        self.committed = True

    async def rollback(self):
        pass

    def collect_new_events(self):
        for product in self.products.seen:
            while product.events:
                event = product.events.popleft()
                self.events_published.append(event)
                yield event


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


class TestChangeBatchQuantity:
    @pytest.mark.asyncio
    async def test_changes_available_quantity(self):
        uow = FakeUnitOfWork()

        await messagebus.handle(
            events.BatchCreated("batch1", "ADORABLE-SETTEE", 100, eta=None),
            uow,
        )

        [batch] = (await uow.products.get(sku="ADORABLE-SETTEE")).batches
        assert batch.available_quantity == 100

        await messagebus.handle(
            events.BatchQuantityChanged("batch1", 50),
            uow,
        )

        assert batch.available_quantity == 50

    @pytest.mark.asyncio
    async def test_reallocates_if_necessary(self):
        uow = FakeUnitOfWork()
        event_history = [
            events.BatchCreated("batch1", "INDIFFERENT-TABLE", 50, None),
            events.BatchCreated("batch2", "INDIFFERENT-TABLE", 50, today),
            events.AllocationRequired("order1", "INDIFFERENT-TABLE", 20),
            events.AllocationRequired("order2", "INDIFFERENT-TABLE", 20),
        ]

        for e in event_history:
            await messagebus.handle(e, uow)

        [batch1, batch2] = (await uow.products.get(sku="INDIFFERENT-TABLE")).batches
        assert batch1.available_quantity == 10
        assert batch2.available_quantity == 50

        await messagebus.handle(events.BatchQuantityChanged("batch1", 25), uow)

        [reallocation_event] = uow.events_published
        assert isinstance(reallocation_event, events.AllocationRequired)
        assert reallocation_event.orderid in {'order1', 'order2'}
        assert reallocation_event.sku == "INDIFFERENT-TABLE"

        # order1 혹은 order2 가 할당 해제된다. 25-20이 수량이 된다.
        assert batch1.available_quantity == 5
        # 다음 배치에서 20을 재할당한다
        assert batch2.available_quantity == 30
