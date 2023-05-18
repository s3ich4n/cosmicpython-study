async def post_to_add_batch(client, ref, sku, qty, eta):
    r = await client.post(
        "http://localhost:13370/batches",
        json={
            'ref': ref,
            'sku': sku,
            'qty': qty,
            'eta': eta,
        },
    )
    assert r.status_code == 201


async def post_to_allocate(client, orderid, sku, qty, expect_success=True):
    r = await client.post(
        "http://localhost:13370/allocate",
        json={
            'orderid': orderid,
            'sku': sku,
            'qty': qty,
        },
    )

    if expect_success:
        assert r.status_code == 202

    return r
