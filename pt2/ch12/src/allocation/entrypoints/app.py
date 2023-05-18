from dependency_injector.wiring import inject, Provide
from fastapi import FastAPI, HTTPException, status, Depends

from pt2.ch12.config import Settings
from pt2.ch12.container import Container
from pt2.ch12.src.allocation import views
from pt2.ch12.src.allocation.adapters import redis

from pt2.ch12.src.allocation.domain import model, events, commands
from pt2.ch12.src.allocation.entrypoints import BatchRequest, OrderLineRequest
from pt2.ch12.src.allocation.service_layer import unit_of_work, messagebus, handlers


# TODO
#   이걸 좀 어떻게 잘 할 방법 없나?
# orm.start_mappers()
app = FastAPI()

container = Container()
container.config.from_pydantic(Settings())
db = container.db()

app.container = container


@app.on_event("startup")
async def on_startup():
    await db.connect(echo=True)
    await db.create_database()
    db.init_session_factory()


@app.on_event("shutdown")
async def on_shutdown():
    await db.disconnect()


@app.post(
    "/batches",
    status_code=status.HTTP_201_CREATED,
)
@inject
async def add_batch_endpoint(
        batch: BatchRequest,
):
    event = commands.CreateBatch(
        ref=batch.ref,
        sku=batch.sku,
        qty=batch.qty,
        eta=batch.eta,
    )
    results = await messagebus.handle(
        event, uow=unit_of_work.SqlAlchemyUnitOfWork(db.session_factory),
    )
    batchref = results.pop(0)
    return {'message': f'Batch added: {batchref}'}


@app.post(
    "/allocate",
    status_code=status.HTTP_202_ACCEPTED,
)
@inject
async def allocate_endpoint(
        order_line: OrderLineRequest,
        channel: redis.AsyncRedis = Depends(Provide[Container.redis]),
):
    try:
        event = commands.Allocate(
            order_id=order_line.orderid,
            sku=order_line.sku,
            qty=order_line.qty,
        )
        batchref = await messagebus.handle(
            event,
            uow=unit_of_work.SqlAlchemyUnitOfWork(db.session_factory),
            channel=channel,
        )
        batchref = batchref.pop(0)

    except (model.OutOfStock, handlers.InvalidSku) as e:
        raise HTTPException(
            detail=str(e),
            status_code=status.HTTP_400_BAD_REQUEST,
        ) from e

    else:
        return {'batchref': batchref}


@app.get(
    "/allocations/{order_id}"
)
@inject
async def allocations_view_endpoint(order_id: str):
    uow = unit_of_work.SqlAlchemyUnitOfWork(db.session_factory)
    result = await views.allocations(order_id, uow)
    if not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
        )

    return result


@app.post(
    "/deallocate",
    status_code=status.HTTP_200_OK,
)
@inject
async def deallocate_endpoint(
        order_line: model.OrderLine,
):
    try:
        event = commands.Deallocate(
            order_id=order_line.orderid,
            sku=order_line.sku,
            qty=order_line.qty,
        )
        deallocated = await messagebus.handle(
            event, uow=unit_of_work.SqlAlchemyUnitOfWork(db.session_factory),
        )
        deallocated = deallocated.pop(0)

    except (model.OutOfStock, handlers.InvalidSku) as e:
        raise HTTPException(
            detail=str(e),
            status_code=status.HTTP_400_BAD_REQUEST,
        ) from e

    else:
        return {"message": f"deallocation done. {deallocated}"}
