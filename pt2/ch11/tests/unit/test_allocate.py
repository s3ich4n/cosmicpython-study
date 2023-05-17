from datetime import datetime, timedelta

import pytest

from pt2.ch11.src.allocation.domain import model, events


today = datetime.now()
tomorrow = today + timedelta(days=1)
later = tomorrow + timedelta(minutes=1)


def test_prefers_current_stock_batches_to_shipments():
    in_stock_batch = model.Batch("in-stock-batch", "RETRO-CLOCK", 100, eta=None)
    shipment_batch = model.Batch("shipment-batch", "RETRO-CLOCK", 100, eta=tomorrow)
    line = model.OrderLine("oref", "RETRO-CLOCK", 10)
    retro_clock = model.Product(
        "RETRO-CLOCK",
        batches=[in_stock_batch, shipment_batch],
    )

    retro_clock.allocate(line)

    assert in_stock_batch.available_quantity == 90
    assert shipment_batch.available_quantity == 100


def test_prefers_earlier_batches():
    earliest = model.Batch("speedy-batch", "MINIMALIST-SPOON", 100, eta=today)
    medium = model.Batch("normal-batch", "MINIMALIST-SPOON", 100, eta=tomorrow)
    latest = model.Batch("slow-batch", "MINIMALIST-SPOON", 100, eta=later)
    line = model.OrderLine("order1", "MINIMALIST-SPOON", 10)
    minimalist_spoon = model.Product(
        "MINIMALIST-SPOON",
        batches=[medium, earliest, latest],
    )

    minimalist_spoon.allocate(line)

    assert earliest.available_quantity == 90
    assert medium.available_quantity == 100
    assert latest.available_quantity == 100


def test_returns_allocated_batch_ref():
    in_stock_batch = model.Batch("in-stock-batch-ref", "HIGHBROW-POSTER", 100, eta=None)
    shipment_batch = model.Batch("shipment-batch-ref", "HIGHBROW-POSTER", 100, eta=tomorrow)
    line = model.OrderLine("oref", "HIGHBROW-POSTER", 10)

    highbrow_poster = model.Product(
        "HIGHBROW-POSTER",
        batches=[in_stock_batch, shipment_batch],
    )

    allocation = highbrow_poster.allocate(line)
    assert allocation == in_stock_batch.reference


def test_raises_out_of_stock_exception_if_cannot_allocate():
    test_batch = model.Batch("batch1", "SMALL-FORK", 10, eta=today)
    out_of_stock_orderline = model.OrderLine("order1", "SMALL-FORK", 11)

    small_fork = model.Product(
        "SMALL-FORK",
        batches=[test_batch],
    )
    small_fork.allocate(out_of_stock_orderline)

    assert small_fork.messages[-1] == events.OutOfStock(sku="SMALL-FORK")
    assert all
