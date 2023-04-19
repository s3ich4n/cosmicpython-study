import pytest
from sqlalchemy.sql import text

from pt1.ch07.src.allocation.domain import model
from pt1.ch07.src.allocation.adapters import repository
from pt1.ch07.tests.integration.conftest import (
    insert_allocation,
    insert_batch,
    insert_order_line,
    insert_product,
)


@pytest.mark.asyncio
async def test_repository_can_save_a_batch(
        session,
        clear,
):
    sku = await insert_product(session, "RUSTY-SOAPDISH")
    batch = model.Batch("batch1", sku, 100, eta=None)
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
async def test_repository_can_retrieve_a_batch_with_allocations(
        session,
        clear,
):
    sku = await insert_product(session, "GENERIC-SOFA")
    orderline_id = await insert_order_line(session, "order1", sku)
    batch1_id = await insert_batch(session, "batch1", sku, 100, None)
    await insert_batch(session, "batch2", sku, 100, None)
    await insert_allocation(session, orderline_id, batch1_id)
    await session.commit()

    repo = repository.SqlAlchemyRepository(session)
    product_retrieved = await repo.get(sku)
    retrieved = product_retrieved.batches[0]

    expected = model.Batch("batch1", sku, 100, eta=None)
    assert retrieved == expected
    assert retrieved.sku == expected.sku
    assert retrieved.purchased_quantity == expected.purchased_quantity
    assert retrieved.allocations == {
        model.OrderLine("order1", "GENERIC-SOFA", 12),
    }


@pytest.mark.asyncio
async def test_repository_can_retrieve_a_list_of_batches(
        session,
        clear,
):
    sku = await insert_product(session, "GENERIC-SOFA")
    test1 = model.Batch("batch11", sku, 10, eta=None)
    test2 = model.Batch("batch12", sku, 10, eta=None)

    repo = repository.SqlAlchemyRepository(session)
    await repo.add(test1)
    await repo.add(test2)
    await session.commit()

    product = await repo.get(sku=sku)
    assert len(product.batches) == 2


@pytest.mark.asyncio
async def test_repository_can_delete_an_order(
        session,
        clear,
):
    sku = await insert_product(session, "TEST-DELETEORDER")
    batch = model.Batch("batch1", sku, 100, eta=None)
    repo = repository.SqlAlchemyRepository(session)
    await repo.add(batch)
    await session.commit()

    repo = repository.SqlAlchemyRepository(session)
    retrieved = await repo.get(sku)
    deleted_batch = retrieved.get_allocation("batch1")

    await repo.delete_batch(deleted_batch)
    await session.commit()

    data = await repo.get(sku)
    assert not data
