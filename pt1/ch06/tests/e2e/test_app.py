from dataclasses import asdict

import httpx
import pytest
from fastapi import status

from pt1.ch06.allocation.domain.model import OrderLine
from pt1.ch06.tests.e2e.conftest import (
    random_sku,
    random_batchref,
    random_orderid,
)


async def post_to_add_batch(ref, sku, qty, eta):
    async with httpx.AsyncClient() as client:
        r = await client.post(
            "http://localhost:13370/batches",
            json={
                'ref': ref,
                'sku': sku,
                'qty': qty,
                'eta': eta,
            },
        )
        assert r.status_code == 201


@pytest.mark.asyncio
@pytest.mark.integration
async def test_happy_path_returns_201_and_allocated_batch(
        client,
):
    sku, othersku = random_sku(), random_sku('other')
    earlybatch = random_batchref(1)
    laterbatch = random_batchref(2)
    otherbatch = random_batchref(3)
    await post_to_add_batch(laterbatch, sku, 100, '2023-04-14')
    await post_to_add_batch(earlybatch, sku, 100, '2023-04-13')
    await post_to_add_batch(otherbatch, othersku, 100, None)

    data = OrderLine(
        orderid=random_orderid(),
        sku=sku,
        qty=3,
    )

    res = await client.post(
        "/allocate",
        json=asdict(data)
    )

    assert res.status_code == status.HTTP_201_CREATED
    assert res.json()['batchref'] == earlybatch


@pytest.mark.asyncio
@pytest.mark.integration
async def test_should_raise_out_of_stock_when_batch_is_invalid(
        client,
):
    order_line = OrderLine("order-123", "DONTEXIST-111", 2)
    res = await client.post(
        "/allocate",
        json=asdict(order_line),
    )

    assert res.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.asyncio
@pytest.mark.integration
async def test_api_do_deallocate(
        client,
        add_stock,
):
    sku, othersku = random_sku(), random_sku('other')
    earlybatch = random_batchref(1)
    laterbatch = random_batchref(2)
    otherbatch = random_batchref(3)
    await add_stock([
        (laterbatch, sku, 100, '2023-04-14'),
        (earlybatch, sku, 100, '2023-04-13'),
        (otherbatch, othersku, 100, None),
    ])

    data = OrderLine(
        orderid=random_orderid(),
        sku=sku,
        qty=3,
    )

    res = await client.post(
        "/deallocate",
        json=asdict(data),
    )

    assert res.status_code == status.HTTP_200_OK


@pytest.mark.asyncio
@pytest.mark.integration
async def test_should_raise_out_of_stock_when_deleted_unknown_batch(
        client,
):
    order_line = OrderLine("order-123", "DONTEXIST-222", 4)
    res = await client.post(
        "/deallocate",
        json=asdict(order_line),
    )

    assert res.status_code == status.HTTP_400_BAD_REQUEST
