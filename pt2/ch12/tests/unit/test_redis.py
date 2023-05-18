import pytest
import pytest_asyncio

from pt2.ch12.src.allocation.adapters import redis

# 컨테이너까지 다 테스트해야됨


@pytest_asyncio.fixture()
async def redis_client():
    session = redis.init_redis_pool(redis_uri="redis://localhost:6379")
    yield session
    await session.aclose()


@pytest.mark.asyncio
async def test_redis_client(redis_client):
    assert redis_client is not None
