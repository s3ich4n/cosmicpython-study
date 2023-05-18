import json
from dataclasses import asdict
from typing import Union

import redis.asyncio as redis
from pydantic import RedisDsn

from pt2.ch12.src.allocation.domain import events


async def init_redis_pool(redis_uri: Union[RedisDsn, str]):
    session = redis.from_url(
        redis_uri,
        encoding="utf-8",
        decode_responses=True,
    )
    yield session
    session.close()
    await session.wait_closed()


class AsyncRedis:
    def __init__(
            self,
            session: redis.Redis,
    ):
        self._session = session

    async def publish(
            self,
            channel,
            event: events.Event,
    ):
        await self._session.publish(channel, json.dumps(asdict(event)))
