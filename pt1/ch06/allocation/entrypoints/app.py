from dependency_injector.wiring import inject, Provide
from fastapi import FastAPI, HTTPException, status, Depends

from pt1.ch06.allocation.adapters import postgres as rdbms_adapter, repository
from pt1.ch06.allocation.entrypoints import schemas as allocations_schema
from pt1.ch06.config import Settings
from pt1.ch06.container import Container
from pt1.ch06.allocation.service_layer import services, unit_of_work
from pt1.ch06.allocation.domain import model


app = FastAPI()

container = Container()
container.config.from_pydantic(Settings())
container.wire(
    modules=[
        __name__,
        rdbms_adapter,
    ]
)

app.container = container
db = container.db()


async def init_session():
    db.init_session_factory()

    async with db.session_factory() as _session:
        yield _session


@app.on_event("startup")
async def on_startup():
    await db.connect(echo=True)


@app.on_event("shutdown")
async def on_shutdown():
    await db.disconnect()


@app.post(
    "/batches",
    status_code=status.HTTP_201_CREATED,
)
@inject
async def add_batch(
        batch: allocations_schema.BatchRequest,
        uow: unit_of_work.AbstractUnitOfWork = Depends(Provide[Container.allocation_uow]),
):
    # FIXME
    await services.add_batch(
        ref=batch.ref,
        sku=batch.sku,
        qty=batch.qty,
        eta=batch.eta,
        uow=uow,
    )


@app.post(
    "/allocate",
    status_code=status.HTTP_201_CREATED,
)
@inject
async def allocate_endpoint(
        order_line: allocations_schema.OrderLineRequest,
        uow: unit_of_work.AbstractUnitOfWork = Depends(Provide[Container.allocation_uow]),
):
    try:
        batchref = await services.allocate(
            orderid=order_line.orderid,
            sku=order_line.sku,
            qty=order_line.qty,
            uow=uow,
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
        uow: unit_of_work.AbstractUnitOfWork = Depends(Provide[Container.allocation_uow]),
):
    try:
        await services.deallocate(
            orderid=order_line.orderid,
            sku=order_line.sku,
            qty=order_line.qty,
            uow=uow,
        )

    except (model.OutOfStock, services.InvalidSku) as e:
        raise HTTPException(
            detail=str(e),
            status_code=status.HTTP_400_BAD_REQUEST,
        ) from e

    else:
        return
