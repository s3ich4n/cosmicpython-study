from datetime import datetime, timedelta

from pt2.ch11.src.allocation.domain import events
from pt2.ch11.src.allocation.domain.model import Batch, Product, OrderLine


today = datetime.now()
tomorrow = today + timedelta(days=1)
later = tomorrow + timedelta(minutes=1)


def test_records_out_of_stock_event_if_cannot_allocate():
    batch = Batch('batch1', 'SMALL-FORK', 10, eta=today)
    product = Product(sku='SMALL-FORK', batches=[batch])
    product.allocate(OrderLine('order1', 'SMALL-FORK', 10))

    allocation = product.allocate(OrderLine('order2', 'SMALL-FORK', 1))

    assert product.messages[-1] == events.OutOfStock(sku="SMALL-FORK")
    assert allocation is None


def test_product_allocate_should_emit_an_event():
    batch = Batch('batch1', 'SMALL-FORK', 10, eta=today)
    product = Product(sku='SMALL-FORK', batches=[batch])
    allocation = product.allocate(OrderLine('order1', 'SMALL-FORK', 1))

    expected_event = events.Allocated(
        orderid="order1",
        sku="SMALL-FORK",
        qty=1,
        batchref=batch.reference,
    )

    assert product.messages[-1] == expected_event
    assert allocation is "batch1"
