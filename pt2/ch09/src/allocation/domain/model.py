from collections import deque
from dataclasses import dataclass
from datetime import date
from typing import Optional, List

from pt2.ch09.src.allocation.domain import events


class OutOfStock(Exception):
    pass


@dataclass(unsafe_hash=True)
class OrderLine:
    orderid: str
    sku: str
    qty: int


class Batch:
    def __init__(
            self,
            ref: str,
            sku: str,
            qty: int,
            eta: Optional[date],
    ):
        self.reference = ref
        self.sku = sku
        self.eta = eta
        self.purchased_quantity = qty
        self.allocations = set()        # OrderLine 값 객체를 모아두는 논리적인 값임! DB 단에선 이게 별도의 allocations로 표현되었음.

    def __repr__(self):
        return f"<Batch {self.reference}>"

    def __eq__(self, other):
        if not isinstance(other, Batch):
            return False
        return other.reference == self.reference

    def __gt__(self, other):
        if self.eta is None:
            return False
        if other.eta is None:
            return True
        return self.eta > other.eta

    def __hash__(self):
        return hash(self.reference)

    def allocate(self, line: OrderLine):
        if self.can_allocate(line):
            self.allocations.add(line)

    def deallocate(self, line: OrderLine):
        if line in self.allocations:
            self.allocations.remove(line)

    @property
    def allocated_quantity(self) -> int:
        return sum(line.qty for line in self.allocations)

    @property
    def available_quantity(self) -> int:
        return self.purchased_quantity - self.allocated_quantity

    def can_allocate(self, line: OrderLine) -> bool:
        return self.sku == line.sku and self.available_quantity >= line.qty


class Product:
    def __init__(
            self,
            sku: str,
            batches: List[Batch],
            version_number: int = 0,
    ):
        self.sku = sku
        self.batches = batches
        self.version_number = version_number
        self.events = deque()    # type: # deque[events.Event]

    def allocate(
            self,
            line: OrderLine,
    ) -> str:
        try:
            batch = next(b for b in sorted(self.batches) if b.can_allocate(line))
            batch.allocate(line)
            self.version_number += 1
            return batch.reference
        except StopIteration:
            self.events.append(events.OutOfStock(line.sku))
            # raise OutOfStock(f"Out of stock for sku {line.sku}")

    def deallocate(
            self,
            line: OrderLine,
    ):
        for batch in self.batches:
            batch.deallocate(line)

    def get_allocation(self, batch_ref: str):
        return next(
            obj for obj in self.batches
            if batch_ref in obj.reference
        )
