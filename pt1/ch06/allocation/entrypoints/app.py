from dependency_injector.wiring import inject, Provide
from fastapi import FastAPI, HTTPException, status, Depends

from pt1.ch06.allocation.adapters import orm
from pt1.ch06.allocation.entrypoints import BatchRequest, OrderLineRequest
from pt1.ch06.config import Settings
from pt1.ch06.container import Container
from pt1.ch06.allocation.service_layer import services, unit_of_work
from pt1.ch06.allocation.domain import model


# TODO
# 이걸 좀 어떻게 잘 할 방법 없나?
orm.start_mappers()

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
async def add_batch(
        batch: BatchRequest,
):
    # FIXME
    await services.add_batch(
        ref=batch.ref,
        sku=batch.sku,
        qty=batch.qty,
        eta=batch.eta,
        uow=unit_of_work.SqlAlchemyUnitOfWork(db.session_factory),
    )


@app.post(
    "/allocate",
    status_code=status.HTTP_201_CREATED,
)
@inject
async def allocate_endpoint(
        order_line: OrderLineRequest,
):
    try:
        batchref = await services.allocate(
            orderid=order_line.orderid,
            sku=order_line.sku,
            qty=order_line.qty,
            uow=unit_of_work.SqlAlchemyUnitOfWork(db.session_factory),
        )

    except (model.OutOfStock, services.InvalidSku) as e:
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
        await services.deallocate(
            orderid=order_line.orderid,
            sku=order_line.sku,
            qty=order_line.qty,
            uow=unit_of_work.SqlAlchemyUnitOfWork(db.session_factory),
        )

    except (model.OutOfStock, services.InvalidSku) as e:
        raise HTTPException(
            detail=str(e),
            status_code=status.HTTP_400_BAD_REQUEST,
        ) from e

    else:
        return
