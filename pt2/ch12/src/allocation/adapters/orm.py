from sqlalchemy.orm import relationship, registry

from sqlalchemy import (
    Column,
    Date,
    event,
    ForeignKey,
    Integer,
    String,
    Table,
)

from pt2.ch12.src.allocation.domain import model


mapper_registry = registry()
metadata = mapper_registry.metadata

order_lines = Table(
    "order_lines",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("sku", String(255)),
    Column("qty", Integer, nullable=False),
    Column("orderid", String(255)),
)

products = Table(
    "products",
    metadata,
    Column("sku", String(255), primary_key=True),
    Column("version_number", Integer, nullable=False, server_default="0"),
)

batches = Table(
    "batches",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("reference", String(255)),
    Column("sku", String(255), ForeignKey("products.sku", ondelete="CASCADE")),
    Column("purchased_quantity", Integer, nullable=False),
    Column("eta", Date, nullable=True),
)

allocations = Table(
    "allocations",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("orderline_id", ForeignKey("order_lines.id")),
    Column("batch_id", ForeignKey("batches.id")),
)


allocations_view = Table(
    'allocations_view',
    metadata,
    Column('orderid', String(255)),
    Column('sku', String(255)),
    Column('batchref', String(255)),
)


def start_mappers():
    lines_mapper = mapper_registry.map_imperatively(
        model.OrderLine,
        order_lines,
    )
    batches_mapper = mapper_registry.map_imperatively(
        model.Batch,
        batches,
        properties={
            "allocations": relationship(
                lines_mapper,
                secondary=allocations,
                collection_class=set,
                lazy="selectin",
            ),
        },
    )
    mapper_registry.map_imperatively(
        model.Product,
        products,
        properties={
            "batches": relationship(
                batches_mapper,
                primaryjoin="batches.c.sku == products.c.sku",
            ),
        },
    )


# 아! 이게 숨어있었다......
@event.listens_for(model.Product, "load")
def receive_load(product, _):
    product.messages = []
