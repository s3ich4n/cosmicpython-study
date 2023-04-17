from datetime import date
from typing import Optional

from pydantic import BaseModel


class OrderLineRequest(BaseModel):
    orderid: str
    sku: str
    qty: int


class BatchRequest(BaseModel):
    ref: str
    sku: str
    qty: int
    eta: Optional[date]
