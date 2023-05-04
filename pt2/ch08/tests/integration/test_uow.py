import asyncio

import pytest
from sqlalchemy import text

from pt2.ch08.src.allocation.domain import model
from pt2.ch08.src.allocation.service_layer import unit_of_work
from pt2.ch08.tests.e2e.conftest import (
    random_batchref,
    random_orderid,
    random_sku,
)
from pt2.ch08.tests.integration.conftest import (
    get_allocated_batch_ref,
    insert_batch,
    insert_product,
    try_to_allocate,
)


@pytest.mark.asyncio
async def test_uow_can_retrieve_a_batch_and_allocate_to_it(
        async_session_maker,
        clear,
):
    session = async_session_maker()
    sku = await insert_product(session, 'HIPSTER-WORKBENCH')
    await insert_batch(session, 'batch1', sku, 100, None)
    await session.commit()

    uow = unit_of_work.SqlAlchemyUnitOfWork(async_session_maker)

    async with uow:
        product = await uow.products.get(sku=sku)
        line = model.OrderLine('o1', sku, 10)
        product.allocate(line)
        await uow.commit()

    batchref = await get_allocated_batch_ref(session, 'o1', sku)
    assert batchref == 'batch1'


@pytest.mark.asyncio
async def test_rolls_back_uncommitted_work_by_default(
        async_session_maker,
        clear,
):
    session = async_session_maker()
    sku = await insert_product(session, 'MEDIUM-PLINTH')
    await insert_batch(session, 'batch1', sku, 100, None)
    # await session.commit()    커밋을 하지 않는 것을 테스트!

    uow = unit_of_work.SqlAlchemyUnitOfWork(async_session_maker)

    async with uow:
        product = await uow.products.get(sku=sku)
        line = model.OrderLine('o1', 'HIPSTER-WORKBENCH', 10)
        product.allocate(line)

    new_session = async_session_maker()
    rows = list(
        await new_session.execute(text('SELECT * FROM batches'))
    )
    assert rows == []


@pytest.mark.asyncio
async def test_rolls_back_on_error(
        async_session_maker,
        clear,
):
    class MyException(Exception):
        pass

    uow = unit_of_work.SqlAlchemyUnitOfWork(async_session_maker)
    with pytest.raises(MyException):
        async with uow:
            sku = await insert_product(uow.session, 'MEDIUM-PLINTH')
            await insert_batch(uow.session, 'batch1', sku, 100, None)
            raise MyException()

    new_session = async_session_maker()
    products_rows = list(
        await new_session.execute(text('SELECT * FROM products'))
    )
    batches_rows = list(
        await new_session.execute(text('SELECT * FROM batches'))
    )

    assert products_rows == []
    assert batches_rows == []


@pytest.mark.asyncio
async def test_concurrent_updates_to_version_are_not_allowed(
        async_session_maker,
        clear,
):
    sku, batch = random_sku(), random_batchref()
    session = async_session_maker()
    async with session.begin():
        sku = await insert_product(session, sku)
        await insert_batch(session, batch, sku, 100, None)
        await session.commit()

    results = await asyncio.gather(
        try_to_allocate(async_session_maker, random_orderid(1), sku),
        try_to_allocate(async_session_maker, random_orderid(2), sku),
        return_exceptions=True,
    )

    for exception in results:
        if exception:
            assert "could not serialize access due to concurrent update" in str(
                exception
            )
        else:
            assert True

    async with session.begin():
        [[version]] = await session.execute(
            text("SELECT version_number FROM products WHERE sku=:sku"),
            dict(sku=sku),
        )
        assert version == 1

        orders = await session.execute(
            text(
                "SELECT orderid FROM allocations"
                " JOIN batches ON allocations.batch_id = batches.id"
                " JOIN order_lines ON allocations.orderline_id = order_lines.id"
                " WHERE order_lines.sku=:sku"
            ),
            dict(sku=sku),
        )
        assert orders.rowcount == 1

    async with unit_of_work.SqlAlchemyUnitOfWork(async_session_maker) as uow:
        await uow.session.execute(text("select 1"))
