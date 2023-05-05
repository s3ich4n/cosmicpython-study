from __future__ import annotations

import asyncio
from collections import deque
from typing import (
    Callable,
    Dict,
    List,
    Type,
    TYPE_CHECKING,
)

from pt2.ch09.src.allocation.domain import events
from pt2.ch09.src.allocation.service_layer import handlers


if TYPE_CHECKING:
    from . import unit_of_work


class AbstractMessageBus:
    HANDLERS: Dict[Type[events.Event], List[Callable]]

    def handle(self, event: events.Event, **kwargs):
        for handler in self.HANDLERS[type(event)]:
            handler(event)


class MessageBus(AbstractMessageBus):
    HANDLERS = {
        events.BatchCreated: [handlers.add_batch],
        events.OutOfStock: [handlers.send_out_of_stock_notification],
        events.AllocationRequired: [handlers.allocate],
        events.DeallocationRequired: [handlers.deallocate],
        events.BatchQuantityChanged: [handlers.change_batch_quantity],
    }


async def handle(
        event: events.Event,
        uow: unit_of_work.AbstractUnitOfWork,
):
    results = deque()
    queue = deque([event])
    while queue:
        event = queue.popleft()
        for handler in MessageBus.HANDLERS[type(event)]:
            task = asyncio.create_task(handler(event, uow=uow))
            results.append(await task)
            queue.extend(uow.collect_new_events())

    return results
