import json

from redis.asyncio import Redis

from pt2.ch12.container import Container

container = Container()

r = Redis.from_url(container.config.broker.REDIS_URI())


async def subscribe_to(channel):
    pubsub = r.pubsub()
    await pubsub.subscribe(channel)
    confirmation = await pubsub.get_message(timeout=3)
    assert confirmation["type"] == "subscribe"
    return pubsub


async def publish_message(channel, message):
    await r.publish(channel, json.dumps(message))
