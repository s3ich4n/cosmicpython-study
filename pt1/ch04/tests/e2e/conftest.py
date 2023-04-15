import uuid

import pytest_asyncio
from sqlalchemy import text


@pytest_asyncio.fixture
async def add_stock(session):
    batches_added = set()
    skus_added = set()

    async def _add_stock(lines):
        for ref, sku, qty, eta in lines:
            await session.execute(
                text(
                    "INSERT INTO"
                    " batches (reference, sku, purchased_quantity, eta)"
                    " VALUES (:ref, :sku, :qty, :eta)"
                ),
                dict(ref=ref, sku=sku, qty=qty, eta=eta),
            )
            [[batch_id]] = await session.execute(
                text(
                    "SELECT id FROM batches"
                    " WHERE reference=:ref AND sku=:sku"
                ),
                dict(ref=ref, sku=sku),
            )
            batches_added.add(batch_id)
            skus_added.add(sku)
        await session.commit()

    yield _add_stock

    for batch_id in batches_added:
        await session.execute(
            text("DELETE FROM allocations WHERE batch_id=:batch_id"),
            dict(batch_id=batch_id),
        )
        await session.execute(
            text("DELETE FROM batches WHERE id=:batch_id"),
            dict(batch_id=batch_id),
        )
    for sku in skus_added:
        await session.execute(
            text("DELETE FROM order_lines WHERE sku=:sku"),
            dict(sku=sku),
        )
        await session.commit()


def random_suffix():
    return uuid.uuid4().hex[:6]


def random_sku(name=""):
    return f"sku-{name}-{random_suffix()}"


def random_batchref(name=""):
    return f"batch-{name}-{random_suffix()}"


def random_orderid(name=""):
    return f"order-{name}-{random_suffix()}"
