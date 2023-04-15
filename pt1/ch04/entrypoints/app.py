from dependency_injector.wiring import Provide, inject
from fastapi import FastAPI, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from pt1.ch04.adapters import postgres as rdbms_adapter
from pt1.ch04.config import Settings
from pt1.ch04.container import Container
from pt1.ch04.service_layer import services
from pt1.ch04.domain import model
from pt1.ch04.adapters import repository


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
    "/allocate",
    status_code=status.HTTP_201_CREATED,
)
@inject
async def allocate_endpoint(
        order_line: model.OrderLine,
        session=Depends(Provide[Container.db]),
):
    session = session.get_session()
    repo = repository.SqlAlchemyRepository(session)

    try:
        batchref = await services.allocate(order_line, repo, session)

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
        session=Depends(Provide[Container.db]),
):
    session = session.get_session()
    repo = repository.SqlAlchemyRepository(session)

    try:
        await services.deallocate(order_line, repo, session)

    except (model.OutOfStock, services.InvalidSku) as e:
        raise HTTPException(
            detail=str(e),
            status_code=status.HTTP_400_BAD_REQUEST,
        ) from e

    else:
        return
