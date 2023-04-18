import pytest
from sqlalchemy import text

from pt1.ch07.src.allocation.domain import model
from pt1.ch07.src.allocation.service_layer import unit_of_work
from pt1.ch07.tests.integration.conftest import (
    get_allocated_batch_ref,
    insert_batch,
    insert_product,
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
        line = model.OrderLine('o1', 'HIPSTER-WORKBENCH', 10)
        product.allocate(line)
        await uow.commit()

    batchref = await get_allocated_batch_ref(session, 'o1', 'HIPSTER-WORKBENCH')
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

    with pytest.raises(model.OutOfStock):
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
