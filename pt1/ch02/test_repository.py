import pytest
from sqlalchemy.sql import text

import model
import repository


async def insert_order_line(session):
    await session.execute(
        text(
            "INSERT INTO order_lines (orderid, sku, qty)"
            ' VALUES ("order1", "GENERIC-SOFA", 12)'
        ),
    )
    [[orderline_id]] = await session.execute(
        text(
            "SELECT id FROM order_lines WHERE orderid=:orderid AND sku=:sku",
        ),
        dict(orderid="order1", sku="GENERIC-SOFA"),
    )
    await session.commit()
    return orderline_id


async def insert_batch(session, batch_id):
    await session.execute(
        text(
            "INSERT INTO batches (reference, sku, purchased_quantity, eta)"
            ' VALUES (:batch_id, "GENERIC-SOFA", 100, null)'
        ),
        dict(batch_id=batch_id),
    )
    [[batch_id]] = await session.execute(
        text(
            'SELECT id FROM batches '
            'WHERE reference=:batch_id AND sku="GENERIC-SOFA"'
        ),
        dict(batch_id=batch_id),
    )
    return batch_id


async def insert_allocation(session, orderline_id, batch_id):
    await session.execute(
        text(
            "INSERT INTO allocations (orderline_id, batch_id)"
            " VALUES (:orderline_id, :batch_id)"
        ),
        dict(orderline_id=orderline_id, batch_id=batch_id),
    )
    await session.commit()


@pytest.mark.asyncio
async def test_repository_can_save_a_batch(session):
    batch = model.Batch("batch1", "RUSTY-SOAPDISH", 100, eta=None)

    repo = repository.SqlAlchemyRepository(session)
    await repo.add(batch)
    await session.commit()

    rows = list(await session.execute(
        text(
            'SELECT reference, sku, purchased_quantity, eta FROM "batches"'
        ),
    ))
    assert rows == [("batch1", "RUSTY-SOAPDISH", 100, None)]


@pytest.mark.asyncio
async def test_repository_can_retrieve_a_batch_with_allocations(session):
    orderline_id = await insert_order_line(session)
    batch1_id = await insert_batch(session, "batch1")
    await insert_batch(session, "batch2")
    await insert_allocation(session, orderline_id, batch1_id)
    repo = repository.SqlAlchemyRepository(session)
    retrieved = await repo.get("batch1")

    expected = model.Batch("batch1", "GENERIC-SOFA", 100, eta=None)
    assert retrieved == expected
    assert retrieved.sku == expected.sku
    assert retrieved.purchased_quantity == expected.purchased_quantity
    assert retrieved.allocations == {
        model.OrderLine("order1", "GENERIC-SOFA", 12),
    }


@pytest.mark.asyncio
async def test_repository_can_retrieve_a_list_of_batches(session):
    await insert_batch(session, "batch1")
    await insert_batch(session, "batch2")

    repo = repository.SqlAlchemyRepository(session)
    assert len(await repo.list()) == 2
