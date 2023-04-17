from datetime import date
from typing import Optional

from pt1.ch07.src.allocation.domain import model
from pt1.ch07.src.allocation.service_layer import unit_of_work


class InvalidSku(Exception):
    ...


async def add_batch(
        ref: str,
        sku: str,
        qty: int,
        eta: Optional[date],
        uow: unit_of_work.AbstractUnitOfWork
):
    async with uow:
        product = uow.products.get(sku=sku)

        if product is None:
            product = model.Product(sku=sku, batches=[])
            uow.products.add(product)

        await product.batches.add(model.Batch(ref, sku, qty, eta))
        await uow.commit()


async def allocate(
        orderid: str,
        sku: str,
        qty: int,
        uow: unit_of_work.AbstractUnitOfWork
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
        product = await uow.products.list()

        if product is None:
            raise InvalidSku(f'Invalid sku {line.sku}')

        model.deallocate(line, product)
        await uow.commit()
