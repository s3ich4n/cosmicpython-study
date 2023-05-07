from collections import deque
from datetime import datetime, timedelta
from typing import List, Dict, Type, Callable, Protocol

import pytest

from pt2.ch10.src.allocation.domain import model, events, commands
from pt2.ch10.src.allocation.service_layer import unit_of_work, handlers, messagebus
from pt2.ch10.src.allocation.adapters import repository
from pt2.ch10.src.allocation.service_layer.messagebus import Message
from pt2.ch10.tests.e2e.conftest import random_batchref


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


class AbstractMessageBus(Protocol):
    HANDLERS: Dict[Type[Message], List[Callable]]

    def handle(self, message: Message):
        for handler in self.HANDLERS[type(message)]:
            handler(message)


class FakeMessageBus(AbstractMessageBus):
    def __init__(self):
        self.events_published: deque[events.Event] = deque()
        self.event_handlers = {
            events.OutOfStock: [handlers.send_out_of_stock_notification],
        }   # type: Dict[Type[events.Event], List[Callable]]
        self.command_handlers = {
            commands.Allocate: [handlers.allocate],
            commands.Deallocate: [handlers.deallocate],
            commands.CreateBatch: [handlers.add_batch],
            commands.ChangeBatchQuantity: [handlers.change_batch_quantity],
        }   # type: Dict[Type[commands.Command], List[Callable]]


class FakeUnitOfWork(unit_of_work.AbstractUnitOfWork):
    def __init__(self):
        self.committed = False
        self.messages_published: deque[events.Event] = deque()
        self.products = repository.TrackingRepository(FakeRepository([]))

    async def commit(self):
        self.committed = True

    async def rollback(self):
        pass

    def collect_new_events(self):
        for product in self.products.seen:
            while product.messages:
                event = product.messages.popleft()
                self.messages_published.append(event)
                yield event


class TestAddBatch:
    @pytest.mark.asyncio
    async def test_add_batch(self):
        uow = FakeUnitOfWork()

        await messagebus.handle(
            commands.CreateBatch("b1", "CRUNCHY-ARMCHAIR", 100, eta=None),
            uow,
        )

        assert await uow.products.get("CRUNCHY-ARMCHAIR") is not None
        assert uow.committed

    @pytest.mark.asyncio
    async def test_returns_allocation(self):
        uow = FakeUnitOfWork()

        await messagebus.handle(
            commands.CreateBatch("b1", "COMPLICATED-LAMP", 100, eta=None),
            uow,
        )
        await messagebus.handle(
            commands.CreateBatch("b2", "COMPLICATED-LAMP", 99, eta=None),
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
            commands.CreateBatch("batch1", "COMPLICATED-LAMP", 100, None),
            uow,
        )
        results = await messagebus.handle(
            commands.Allocate("o1", "COMPLICATED-LAMP", 10),
            uow,
        )

        assert results.popleft() == "batch1"

    @pytest.mark.asyncio
    async def test_error_for_invalid_sku(self):
        uow = FakeUnitOfWork()

        with pytest.raises(handlers.InvalidSku, match="Invalid sku NONEXISTENTSKU"):
            await messagebus.handle(
                commands.CreateBatch("b1", "AREALSKU", 100, eta=None),
                uow,
            )
            await messagebus.handle(
                commands.Allocate("o1", "NONEXISTENTSKU", 10),
                uow,
            )

    @pytest.mark.asyncio
    async def test_commits(self):
        uow = FakeUnitOfWork()

        await messagebus.handle(
            commands.CreateBatch("b1", "OMNIOUS-MIRROR", 100, eta=None),
            uow,
        )
        await messagebus.handle(
            commands.Allocate("o1", "OMNIOUS-MIRROR", 10),
            uow,
        )

        assert uow.committed is True

    @pytest.mark.asyncio
    async def test_prefers_current_stock_batches_to_shipments(self):
        uow = FakeUnitOfWork()
        in_stock_batch = random_batchref(1)
        shipment_batch = random_batchref(2)

        await messagebus.handle(
            commands.CreateBatch(in_stock_batch, "RETRO-CLOCK", 100, eta=None),
            uow,
        )
        await messagebus.handle(
            commands.CreateBatch(shipment_batch, "RETRO-CLOCK", 100, eta=None),
            uow,
        )
        await messagebus.handle(
            commands.Allocate("oref", "RETRO-CLOCK", 10),
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
            commands.CreateBatch("o1", "DEALLOC-TEST", 100, eta=None),
            uow,
        )
        product = await uow.products.get("DEALLOC-TEST")
        batch = product.batches[0]

        result = await messagebus.handle(
            commands.Allocate("o1", "DEALLOC-TEST", 10),
            uow,
        )
        result = result.popleft()
        assert result == "o1"
        assert batch.allocated_quantity == 10

        await messagebus.handle(
            commands.Deallocate("o1", "DEALLOC-TEST", 10),
            uow,
        )

        assert batch.allocated_quantity == 0
        assert uow.committed


class TestChangeBatchQuantity:
    @pytest.mark.asyncio
    async def test_changes_available_quantity(self):
        uow = FakeUnitOfWork()

        await messagebus.handle(
            commands.CreateBatch("batch1", "ADORABLE-SETTEE", 100, eta=None),
            uow,
        )

        [batch] = (await uow.products.get(sku="ADORABLE-SETTEE")).batches
        assert batch.available_quantity == 100

        await messagebus.handle(
            commands.ChangeBatchQuantity("batch1", 50),
            uow,
        )

        assert batch.available_quantity == 50

    @pytest.mark.asyncio
    async def test_reallocates_if_necessary(self):
        uow = FakeUnitOfWork()
        event_history = [
            commands.CreateBatch("batch1", "INDIFFERENT-TABLE", 50, None),
            commands.CreateBatch("batch2", "INDIFFERENT-TABLE", 50, today),
            commands.Allocate("order1", "INDIFFERENT-TABLE", 20),
            commands.Allocate("order2", "INDIFFERENT-TABLE", 20),
        ]

        for e in event_history:
            await messagebus.handle(e, uow)

        [batch1, batch2] = (await uow.products.get(sku="INDIFFERENT-TABLE")).batches
        assert batch1.available_quantity == 10
        assert batch2.available_quantity == 50

        await messagebus.handle(commands.ChangeBatchQuantity("batch1", 25), uow)

        [reallocation_event] = uow.messages_published
        assert isinstance(reallocation_event, commands.Allocate)
        assert reallocation_event.order_id in {'order1', 'order2'}
        assert reallocation_event.sku == "INDIFFERENT-TABLE"

        # order1 혹은 order2 가 할당 해제된다. 25-20이 수량이 된다.
        assert batch1.available_quantity == 5
        # 다음 배치에서 20을 재할당한다
        assert batch2.available_quantity == 30
