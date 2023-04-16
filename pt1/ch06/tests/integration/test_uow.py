import pytest
from sqlalchemy import text

from pt1.ch06.allocation.domain import model
from pt1.ch06.allocation.service_layer import unit_of_work
from pt1.ch06.tests.integration.conftest import (
    insert_batch,
    get_allocated_batch_ref,
)


@pytest.mark.asyncio
async def test_uow_can_retrieve_a_batch_and_allocate_to_it(session_factory):
    session = session_factory()
    insert_batch(session, 'batch1', 'HIPSTER-WORKBENCH', 100, None)
    session.commit()

    uow = unit_of_work.SqlAlchemyUnitOfWork(session_factory)

    with uow:
        batch = uow.batches.get(reference='batch1')
        line = model.OrderLine('o1', 'HIPSTER-WORKBENCH', 10)
        batch.allocate(line)
        uow.commit()

    batchref = get_allocated_batch_ref(session, 'o1', 'HIPSTER-WORKBENCH')
    assert batchref == 'batch1'


@pytest.mark.asyncio
async def test_rolls_back_uncommitted_work_by_default(session_factory):
    uow = unit_of_work.SqlAlchemyUnitOfWork(session_factory)
    async with uow:
        insert_batch(uow._session, 'batch1', 'MEDIUM-PLINTH', 100, None)

    new_session = session_factory()
    rows = list(
        await new_session.execute(text('SELECT * FROM batches'))
    )
    assert rows == []


@pytest.mark.asyncio
async def test_rolls_back_on_error(session_factory):
    class MyException(Exception):
        pass

    uow = unit_of_work.SqlAlchemyUnitOfWork(session_factory)
    with pytest.raises(MyException):
        async with uow:
            insert_batch(uow._session, 'batch1', 'MEDIUM-PLINTH', 100, None)
            raise MyException()

    new_session = session_factory()
    rows = list(
        await new_session.execute(text('SELECT * FROM batches'))
    )
    assert rows == []
