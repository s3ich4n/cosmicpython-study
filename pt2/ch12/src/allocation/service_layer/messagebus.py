from __future__ import annotations

import logging
from typing import (
    Callable,
    Dict,
    List,
    Optional,
    TYPE_CHECKING,
    Type,
    Union,
)


from pt2.ch12.src.allocation.adapters import redis
from pt2.ch12.src.allocation.domain import events, commands
from pt2.ch12.src.allocation.service_layer import handlers

if TYPE_CHECKING:
    from . import unit_of_work


logger = logging.getLogger(__name__)
Message = Union[commands.Command, events.Event]


class MessageBus:
    EVENT_HANDLERS = {
        events.Allocated: [
            handlers.publish_allocate_event,
            handlers.add_allocation_to_read_model,
        ],
        events.Deallocated: [
            handlers.remove_allocation_from_read_model,
            handlers.reallocate,
        ],
        events.OutOfStock: [handlers.send_out_of_stock_notification],
    }   # type: Dict[Type[events.Event], List[Callable]]
    COMMAND_HANDLERS = {
        commands.Allocate: handlers.allocate,
        commands.Deallocate: handlers.deallocate,
        commands.CreateBatch: handlers.add_batch,
        commands.ChangeBatchQuantity: handlers.change_batch_quantity,
    }   # type: Dict[Type[commands.Command], Callable]


async def handle(
        message: Message,
        uow: unit_of_work.AbstractUnitOfWork,
        channel: Optional[redis.AsyncRedis, None] = None,
):
    results = []
    queue: List[Message] = [message]
    while queue:
        message = queue.pop(0)

        if isinstance(message, events.Event):
            await handle_event(message, queue, uow, channel)
        elif isinstance(message, commands.Command):
            result = await handle_command(message, queue, uow)
            results.append(result)
        else:
            raise Exception(f'{message} was not a Command or Event')

    return results


async def handle_command(
        command: commands.Command,
        queue: List[Message],
        uow: unit_of_work.AbstractUnitOfWork,
):
    logger.debug(f'Handling command {command}')
    try:
        handler = MessageBus.COMMAND_HANDLERS[type(command)]
        result = await handler(command, uow)
        queue.extend(uow.collect_new_events())
        return result
    except Exception as ex:
        logger.exception(f'Exception handling {command}... detail: {ex}')
        raise


async def handle_event(
        event: events.Event,
        queue: List[Message],
        uow: unit_of_work.AbstractUnitOfWork,
        channel: redis.AsyncRedis,
):
    for handler in MessageBus.EVENT_HANDLERS[type(event)]:
        try:
            logger.debug(f'Handling event {event} with {handler}')
            await handler(event, uow, channel)
            queue.extend(uow.collect_new_events())
        except Exception as ex:
            logger.exception(f'Exception handling {event}... detail: {ex}')
            continue
