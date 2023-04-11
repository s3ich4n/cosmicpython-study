from collections import namedtuple
from dataclasses import dataclass
from datetime import date
from typing import Optional, NamedTuple, List


@dataclass(unsafe_hash=True)
class OrderLine:
    orderid: str
    sku: str
    qty: int


@dataclass(frozen=True)
class Name:
    first_name: str
    surname: str


@dataclass(frozen=True)
class Money:
    currency: str
    value: int

    def __add__(self, other) -> 'Money':
        if other.currency != self.currency:
            raise ValueError(f"Cannot add {self.currency} to {other.currency}")
        return Money(self.currency, self.value + other.value)

    def __sub__(self, other) -> 'Money':
        if other.currency != self.currency:
            raise ValueError(f"Cannot subtract {self.currency} to {other.currency}")
        return Money(self.currency, self.value - other.value)

    def __mul__(self, number):
        return Money(self.currency, self.value * number)


Line = namedtuple('Line', ['sku', 'qty'])


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
        self._purchased_quantity = qty
        self._allocations = set()  # type: Set[OrderLine]

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
            self._allocations.add(line)

    def deallocate(self, line: OrderLine):
        if line in self._allocations:
            self._allocations.remove(line)

    @property
    def allocated_quantity(self) -> int:
        return sum(line.qty for line in self._allocations)

    @property
    def available_quantity(self) -> int:
        return self._purchased_quantity - self.allocated_quantity

    def can_allocate(self, line: OrderLine) -> bool:
        return self.sku == line.sku and self.available_quantity >= line.qty


def allocate(line: OrderLine, batches: List[Batch]) -> str:
    try:
        batch = next(b for b in sorted(batches) if b.can_allocate(line))
        batch.allocate(line)
        return batch.reference
    except StopIteration:
        raise OutOfStock(f"Out of stock for sku {line.sku}")


class OutOfStock(Exception):
    pass
