from dependency_injector.wiring import inject
from fastapi import FastAPI, HTTPException, status

from pt2.ch10.config import Settings
from pt2.ch10.container import Container

from pt2.ch10.src.allocation.domain import model, events, commands
from pt2.ch10.src.allocation.entrypoints import BatchRequest, OrderLineRequest
from pt2.ch10.src.allocation.service_layer import unit_of_work, messagebus, handlers


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
    batchref = results.popleft()
    return {'message': f'Batch added: {batchref}'}


@app.post(
    "/allocate",
    status_code=status.HTTP_201_CREATED,
)
@inject
async def allocate_endpoint(
        order_line: OrderLineRequest,
):
    try:
        event = commands.Allocate(
            order_id=order_line.orderid,
            sku=order_line.sku,
            qty=order_line.qty,
        )
        batchref = await messagebus.handle(
            event, uow=unit_of_work.SqlAlchemyUnitOfWork(db.session_factory),
        )
        batchref = batchref.popleft()

    except (model.OutOfStock, handlers.InvalidSku) as e:
        raise HTTPException(
            detail=str(e),
            status_code=status.HTTP_400_BAD_REQUEST,
        ) from e

    else:
        return {'batchref': batchref}


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
        deallocated = deallocated.popleft()

    except (model.OutOfStock, handlers.InvalidSku) as e:
        raise HTTPException(
            detail=str(e),
            status_code=status.HTTP_400_BAD_REQUEST,
        ) from e

    else:
        return {"message": f"deallocation done. {deallocated}"}
