from datetime import date

import pytest

from pt2.ch12.src.allocation import views
from pt2.ch12.src.allocation.domain import commands
from pt2.ch12.src.allocation.service_layer import (
    messagebus,
    unit_of_work,
)

today = date.today()


@pytest.mark.asyncio
async def test_allocations_view(sqlite_session_factory):
    uow = unit_of_work.SqlAlchemyUnitOfWork(sqlite_session_factory)
    await messagebus.handle(commands.CreateBatch('sku1batch', 'sku1', 50, None), uow)
    await messagebus.handle(commands.CreateBatch('sku2batch', 'sku2', 50, today), uow)
    await messagebus.handle(commands.Allocate('order1', 'sku1', 20), uow)
    await messagebus.handle(commands.Allocate('order1', 'sku2', 20), uow)

    # 제대로 데이터를 얻는지 보기 위해 여러 배치와 주문을 추가
    await messagebus.handle(commands.CreateBatch('sku1batch-l8r', 'sku1', 50, today), uow)
    await messagebus.handle(commands.Allocate('otherorder', 'sku1', 30), uow)
    await messagebus.handle(commands.Allocate('otherorder', 'sku2', 10), uow)

    # Example 02
    # view_result = await views.allocations('order1', uow)
    # expected = [
    #     {'batchref': 'sku1batch', 'sku': 'sku1'},
    #     {'batchref': 'sku2batch', 'sku': 'sku2'},
    # ]
    #
    # for data in expected:
    #     assert data in view_result

    assert await views.allocations("order1", uow) == [
        {"sku": "sku1", "batchref": "sku1batch"},
        {"sku": "sku2", "batchref": "sku2batch"},
    ]


@pytest.mark.asyncio
async def test_deallocation(sqlite_session_factory):
    uow = unit_of_work.SqlAlchemyUnitOfWork(sqlite_session_factory)
    await messagebus.handle(commands.CreateBatch("b1", "sku1", 50, None), uow)
    await messagebus.handle(commands.CreateBatch("b2", "sku1", 50, today), uow)
    await messagebus.handle(commands.Allocate("o1", "sku1", 40), uow)
    await messagebus.handle(commands.ChangeBatchQuantity("b1", 10), uow)

    # Example 02
    # view_result = await views.allocations("o1", uow)
    # expected = [
    #     {"batchref": "b2", "sku": "sku1", },
    # ]
    #
    # assert view_result in expected

    assert await views.allocations("o1", uow) == [
        {"sku": "sku1", "batchref": "b2"},
    ]
