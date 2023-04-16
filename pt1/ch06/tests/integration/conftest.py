import pytest_asyncio
from sqlalchemy.sql import text


async def reset_table(session):
    await session.execute(text("DELETE FROM order_lines"))
    await session.execute(text("DELETE FROM batches"))
    await session.execute(text("DELETE FROM allocations"))
    await session.commit()


@pytest_asyncio.fixture(name="saving_a_batch")
async def data_factory_saving_a_batch(session):
    yield
    await reset_table(session)


@pytest_asyncio.fixture(name="a_batch_with_allocations")
async def data_factory_a_batch_with_allocations(session):
    orderline_id = await insert_order_line(session)
    batch1_id = await insert_batch(session, "batch1")
    await insert_batch(session, "batch2")
    await insert_allocation(session, orderline_id, batch1_id)

    yield
    await reset_table(session)


@pytest_asyncio.fixture(name="multiple_batches")
async def data_factory_multiple_batches(session):
    yield
    await reset_table(session)


@pytest_asyncio.fixture(name="batch_deletion")
async def data_factory_a_batch_wants_to_delete_an_order(session):
    orderline_id = await insert_order_line(session)
    batch1_id = await insert_batch(session, "batch1")
    await insert_batch(session, "batch2")
    await insert_allocation(session, orderline_id, batch1_id)

    yield
    await reset_table(session)


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


async def get_allocated_batch_ref(session, orderline_id, sku):
    [[orderlineid]] = await session.execute(
        text(
            "SELECT id FROM order_lines WHERE orderid=:orderid AND sku=:sku"
        ),
        dict(orderid=orderline_id, sku=sku),
    )
    [[batchref]] = await session.execute(
        text(
            "SELECT b.reference FROM allocations"
            " JOIN batches AS b ON batch_id = b.id"
            " WHERE orderline_id=:orderlineid"
        ),
        dict(orderlineid=orderlineid),
    )
    return batchref
