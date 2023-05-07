import pytest_asyncio
from sqlalchemy.sql import text


@pytest_asyncio.fixture(name="session")
async def get_async_session(
        async_session_maker,
):
    async with async_session_maker() as _session:
        yield _session


async def insert_order_line(session, order_id, sku):
    await session.execute(
        text(
            "INSERT INTO order_lines (orderid, sku, qty)"
            " VALUES ('order1', 'GENERIC-SOFA', 12)"
        ),
    )
    [[orderline_id]] = await session.execute(
        text(
            "SELECT id FROM order_lines WHERE orderid=:orderid AND sku=:sku",
        ),
        dict(orderid=order_id, sku=sku),
    )
    await session.commit()
    return orderline_id


async def insert_product(session, sku):
    await session.execute(
        text(
            "INSERT INTO products (sku)"
            " VALUES (:sku)"
        ),
        dict(sku=sku),
    )
    [[product_sku]] = await session.execute(
        text(
            'SELECT sku FROM products'
            ' WHERE sku=:sku'
        ),
        dict(sku=sku),
    )
    return product_sku


async def insert_batch(session, ref, sku, qty, eta):
    await session.execute(
        text(
            "INSERT INTO batches (reference, sku, purchased_quantity, eta)"
            ' VALUES (:ref, :sku, :qty, :eta)'
        ),
        dict(ref=ref, sku=sku, qty=qty, eta=eta),
    )
    [[batch_id]] = await session.execute(
        text(
            'SELECT id FROM batches'
            ' WHERE reference=:batch_id AND sku=:sku'
        ),
        dict(batch_id=ref, sku=sku),
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
