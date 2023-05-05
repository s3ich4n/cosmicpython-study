from __future__ import annotations

from typing import TYPE_CHECKING

from pt2.ch09.src.allocation.domain import model
from pt2.ch09.src.allocation.adapters import email
from pt2.ch09.src.allocation.domain import events


if TYPE_CHECKING:
    from . import unit_of_work


class InvalidSku(Exception):
    ...


async def add_batch(
        event: events.BatchCreated,
        uow: unit_of_work.AbstractUnitOfWork,
):
    async with uow:
        product = await uow.products.get(sku=event.sku)

        if product is None:
            product = model.Product(sku=event.sku, batches=[])
            await uow.products.add(product)

        product.batches.append(
            model.Batch(
                reference=event.ref,
                sku=event.sku,
                qty=event.qty,
                eta=event.eta,
            )
        )
        await uow.commit()


async def allocate(
        event: events.AllocationRequired,
        uow: unit_of_work.AbstractUnitOfWork,
) -> str:
    line = model.OrderLine(event.orderid, event.sku, event.qty)

    async with uow:
        product = await uow.products.get(sku=line.sku)
        if product is None:
            raise InvalidSku(f'Invalid sku {line.sku}')

        batchref = product.allocate(line)
        await uow.commit()

    return batchref


async def send_out_of_stock_notification(
        event: events.OutOfStock,
        uow: unit_of_work.AbstractUnitOfWork,
):
    await email.send_mail(
        'stock_admin@made.com',
        f'out of stock for {event.sku}',
    )


async def deallocate(
        event: events.DeallocationRequired,
        uow: unit_of_work.AbstractUnitOfWork,
):
    line = model.OrderLine(event.orderid, event.sku, event.qty)

    async with uow:
        product = await uow.products.get(sku=line.sku)

        if product is None:
            raise InvalidSku(f'Invalid sku {line.sku}')

        product.deallocate(line)
        await uow.commit()


async def change_batch_quantity(
        event: events.BatchQuantityChanged,
        uow: unit_of_work.AbstractUnitOfWork,
):
    async with uow:
        product = await uow.products.get_by_batchref(batchref=event.ref)
        product.change_batch_quantity(event.ref, event.qty)
        await uow.commit()
