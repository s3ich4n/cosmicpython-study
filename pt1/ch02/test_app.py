from dataclasses import asdict

import pytest
from fastapi import status

from model import OrderLine, OutOfStock


@pytest.mark.asyncio
@pytest.mark.integration
async def test_should_raise_out_of_stock_when_batch_is_none(
        client,
):
    # Arrange
    order_line = OrderLine("order-123", "batch-001", 2)

    with pytest.raises(OutOfStock):
        res = await client.post(
            "/allocate",
            json=asdict(order_line),
        )

        assert res.status_code == status.HTTP_404_NOT_FOUND
