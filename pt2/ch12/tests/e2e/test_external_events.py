import json

import async_timeout
import pytest

from pt2.ch12.tests.e2e import redis_client
from pt2.ch12.tests.e2e.api_client import (
    post_to_add_batch,
    post_to_allocate,
)
from pt2.ch12.tests.e2e.conftest import (
    random_batchref,
    random_orderid,
    random_sku,
)


@pytest.mark.asyncio
async def test_change_batch_quantity_leading_to_reallocation(client):
    # 두 배치와 할당을 수행하여 한 쪽에 할당하는 주문으로 시작한다.
    orderid, sku = random_orderid(), random_sku()
    earlier_batch, later_batch = random_batchref("old"), random_batchref("newer")
    await post_to_add_batch(client, earlier_batch, sku, qty=10, eta="2023-01-01")
    await post_to_add_batch(client, later_batch, sku, qty=10, eta="2023-01-02")
    response = await post_to_allocate(client, orderid, sku, 10)
    assert response.json()["batchref"] == earlier_batch

    subscription = await redis_client.subscribe_to("line_allocated")

    await redis_client.publish_message(
        "line_allocated",
        {
            'batchref': later_batch,
            'orderid': orderid,
            'qty': 10,
            'sku': sku,
        },
    )

    messages = []
    async with async_timeout.timeout(3):
        message = await subscription.get_message(timeout=1)
        if message:
            messages.append(message)
            print(messages)

    assert len(messages) == 1
    data = json.loads(messages[-1]["data"])
    assert data['orderid'] == orderid
    assert data["batchref"] == later_batch
