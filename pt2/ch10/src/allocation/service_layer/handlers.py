from __future__ import annotations

from typing import TYPE_CHECKING

from pt2.ch10.src.allocation.domain import model
from pt2.ch10.src.allocation.adapters import email
from pt2.ch10.src.allocation.domain import commands, events

if TYPE_CHECKING:
    from . import unit_of_work


class InvalidSku(Exception):
    ...


async def add_batch(
        command: commands.CreateBatch,
        uow: unit_of_work.AbstractUnitOfWork,
):
    async with uow:
        product = await uow.products.get(sku=command.sku)

        if product is None:
            product = model.Product(sku=command.sku, batches=[])
            await uow.products.add(product)

        product.batches.append(
            model.Batch(
                reference=command.ref,
                sku=command.sku,
                qty=command.qty,
                eta=command.eta,
            )
        )
        await uow.commit()


async def allocate(
        command: commands.Allocate,
        uow: unit_of_work.AbstractUnitOfWork,
) -> str:
    line = model.OrderLine(command.order_id, command.sku, command.qty)

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
        command: commands.Deallocate,
        uow: unit_of_work.AbstractUnitOfWork,
):
    line = model.OrderLine(command.order_id, command.sku, command.qty)

    async with uow:
        product = await uow.products.get(sku=line.sku)

        if product is None:
            raise InvalidSku(f'Invalid sku {line.sku}')

        product.deallocate(line)
        await uow.commit()


async def change_batch_quantity(
        command: commands.ChangeBatchQuantity,
        uow: unit_of_work.AbstractUnitOfWork,
):
    async with uow:
        product = await uow.products.get_by_batchref(batchref=command.ref)
        product.change_batch_quantity(command.ref, command.qty)
        await uow.commit()
