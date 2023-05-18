from sqlalchemy import select

from pt2.ch12.src.allocation.domain import model
from pt2.ch12.src.allocation.service_layer import unit_of_work
from sqlalchemy.sql import text


# Example 01
#
# async def allocations(
#         orderid: str,
#         uow: unit_of_work.SqlAlchemyUnitOfWork,
# ):
#     async with uow:
#         results = await uow.session.execute(
#             text(
#                 """
#                 SELECT ol.sku, b.reference
#                 FROM allocations AS a
#                 JOIN batches as b ON a.batch_id = b.id
#                 JOIN order_lines AS ol ON a.orderline_id = ol.id
#                 WHERE ol.orderid = :orderid
#                 """
#             ),
#             dict(orderid=orderid),
#         )
#
#     return results.mappings().all()

# Example 02
#
# async def allocations(orderid: str, uow: unit_of_work.SqlAlchemyUnitOfWork):
#     async with uow:
#         batches = (
#             await uow.session.execute(
#                 select(model.Batch)
#                 .join(model.OrderLine, model.OrderLine.orderid == orderid)
#             )
#         ).scalars()
#
#         return [
#             {'batchref': b.reference, 'sku': b.sku}
#             for b in batches
#         ]


async def allocations(
        orderid: str,
        uow: unit_of_work.SqlAlchemyUnitOfWork,
):
    async with uow:
        results = await uow.session.execute(
            text(
                """
                SELECT batchref, sku
                FROM allocations_view
                WHERE orderid = :orderid
                """
            ),
            dict(orderid=orderid),
        )

    return results.mappings().all()
