from __future__ import annotations

from dataclasses import asdict
from typing import TYPE_CHECKING

from sqlalchemy import text

from pt2.ch12.src.allocation.adapters import (
    email,
    redis,
)
from pt2.ch12.src.allocation.domain import (
    commands,
    events,
    model,
)


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


async def reallocate(
    event: events.Deallocated,
    uow: unit_of_work.AbstractUnitOfWork,
):
    async with uow:
        product = await uow.products.get(sku=event.sku)
        product.messages.append(commands.Allocate(**asdict(event)))
        await uow.commit()


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


async def publish_allocate_event(
        event: events.Allocated,
        uow: unit_of_work.AbstractUnitOfWork,
        channel: redis.AsyncRedis,
):
    await channel.publish("line_allocated", event)


async def add_allocation_to_read_model(
        event: events.Allocated,
        uow: unit_of_work.SqlAlchemyUnitOfWork,
):
    async with uow:
        await uow.session.execute(
            text(
                """
                INSERT INTO allocations_view (orderid, sku, batchref)
                VALUES (:orderid, :sku, :batchref)
                """
            ),
            dict(
                orderid=event.orderid,
                sku=event.sku,
                batchref=event.batchref,
            )
        )
        await uow.commit()


async def remove_allocation_from_read_model(
        event: events.Deallocated,
        uow: unit_of_work.SqlAlchemyUnitOfWork,
):
    async with uow:
        await uow.session.execute(
            text(
                "DELETE FROM allocations_view"
                " WHERE orderid = :orderid AND sku = :sku"
            ),
            dict(
                orderid=event.orderid,
                sku=event.sku,
            )
        )
        await uow.commit()
