import pytest
from sqlalchemy.sql import text

from pt1.ch04.domain import model
from pt1.ch04.adapters import repository


@pytest.mark.asyncio
async def test_repository_can_save_a_batch(
        session,
        saving_a_batch,
):
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
async def test_repository_can_retrieve_a_batch_with_allocations(
        session,
        a_batch_with_allocations,
):
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
async def test_repository_can_retrieve_a_list_of_batches(
        session,
        multiple_batches,
):
    test1 = model.Batch("batch11", "GENERIC-SOFA", 10, eta=None)
    test2 = model.Batch("batch12", "GENERIC-SOFA", 10, eta=None)

    repo = repository.SqlAlchemyRepository(session)
    await repo.add(test1)
    await repo.add(test2)
    await session.commit()

    data = await repo.list()
    assert len(data) == 2


@pytest.mark.asyncio
async def test_repository_can_delete_an_order(
        session,
        batch_deletion,
):
    repo = repository.SqlAlchemyRepository(session)
    retrieved = await repo.get("batch1")

    await repo.delete_batch(retrieved)
    await session.commit()

    data = await repo.get("batch1")
    assert not data
