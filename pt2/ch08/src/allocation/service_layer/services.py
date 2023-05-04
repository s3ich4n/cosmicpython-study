from datetime import date
from typing import Optional

from pt2.ch08.src.allocation.domain import model
from pt2.ch08.src.allocation.service_layer import unit_of_work


class InvalidSku(Exception):
    ...


async def add_batch(
        ref: str,
        sku: str,
        qty: int,
        eta: Optional[date],
        uow: unit_of_work.AbstractUnitOfWork,
):
    async with uow:
        product = await uow.products.get(sku=sku)

        if product is None:
            product = model.Product(sku=sku, batches=[])
            await uow.products.add(product)

        product.batches.append(model.Batch(ref, sku, qty, eta))
        await uow.commit()


async def allocate(
        orderid: str,
        sku: str,
        qty: int,
        uow: unit_of_work.AbstractUnitOfWork,
) -> str:
    line = model.OrderLine(orderid, sku, qty)

    async with uow:
        product = await uow.products.get(sku=line.sku)
        if product is None:
            raise InvalidSku(f'Invalid sku {line.sku}')

        batchref = product.allocate(line)
        await uow.commit()

    return batchref


async def deallocate(
        orderid: str,
        sku: str,
        qty: int,
        uow: unit_of_work.AbstractUnitOfWork,
):
    line = model.OrderLine(orderid, sku, qty)

    async with uow:
        product = await uow.products.get(sku=line.sku)

        if product is None:
            raise InvalidSku(f'Invalid sku {line.sku}')

        product.deallocate(line)
        await uow.commit()
