from pt1.ch04.domain import model
from pt1.ch04.adapters import repository


class InvalidSku(Exception):
    ...


def is_valid_sku(sku, batches):
    return sku in {b.sku for b in batches}


async def allocate(
        line: model.OrderLine,
        repo: repository.AbstractRepository,
        session,
) -> str:
    """ batches를 line에 할당한다.

    FYI,
        의존성 역전 원칙이 여기 들어감에 유의!
        고수준 모듈인 서비스 계층은 저장소라는 추상화에 의존한다.
        구현의 세부내용은 어떤 영속 저장소를 선택했느냐에 따라 다르지만
        같은 추상화에 의존한다.

    :param line:
    :param repo:
    :param session:
    :return:
    """
    batches = await repo.list()
    if not is_valid_sku(line.sku, batches):
        raise InvalidSku(f'Invalid sku {line.sku}')
    batchref = model.allocate(line, batches)
    await session.commit()

    return batchref


async def deallocate(
        line: model.OrderLine,
        repo: repository.AbstractRepository,
        session,
):
    batches = await repo.list()
    if not is_valid_sku(line.sku, batches):
        raise InvalidSku(f'Invalid sku {line.sku}')
    model.deallocate(line, batches)
    await session.commit()
