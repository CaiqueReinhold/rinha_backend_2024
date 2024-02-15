from io import BytesIO

import orjson
import uvicorn
from asyncpg import create_pool

from services import create_transaction, get_statement

pool = None


async def trasaction(client_id, body, send, conn):
    status_code = 200
    res = {}
    try:
        v = body.get("valor")
        d = body.get("descricao")
        t = body.get("tipo")

        validation_error = (
            (d is None)
            or (d == "")
            or (len(d) > 10)
            or (v is None)
            or (type(v) is not int)
            or (t is None)
            or (t not in ["c", "d"])
        )
        if validation_error:
            raise ValueError()

        res = await create_transaction(
            conn,
            client_id,
            v,
            t,
            d,
        )
    except KeyError:
        status_code = 404
    except ValueError:
        status_code = 422

    res = orjson.dumps(res)
    await send(
        {
            "type": "http.response.start",
            "status": status_code,
            "headers": [
                [b"content-type", b"application/json"],
                [b"content-length", str(len(res)).encode("utf-8")],
            ],
        }
    )
    await send(
        {
            "type": "http.response.body",
            "body": res,
        }
    )


async def statement(client_id, send, conn):
    status_code = 200
    res = {}

    try:
        res = await get_statement(conn, client_id)
    except KeyError:
        status_code = 404

    res = orjson.dumps(res)
    await send(
        {
            "type": "http.response.start",
            "status": status_code,
            "headers": [
                [b"content-type", b"application/json"],
                [b"content-length", str(len(res)).encode("utf-8")],
            ],
        }
    )
    await send(
        {
            "type": "http.response.body",
            "body": res,
        }
    )


async def read_body(receive):
    body = BytesIO()
    more_body = True

    while more_body:
        message = await receive()
        body.write(message.get("body", b""))
        more_body = message.get("more_body", False)

    return body.getvalue().decode("utf-8")


async def init():
    global pool
    pool = await create_pool(
        user="admin",
        password="123",
        database="rinha",
        host="db",
        port=5432,
    )


async def app(scope, receive, send):
    if scope["type"] == "lifespan":
        while True:
            message = await receive()
            if message["type"] == "lifespan.startup":
                await init()
                await send({"type": "lifespan.startup.complete"})
            elif message["type"] == "lifespan.shutdown":
                await pool.close()
                await send({"type": "lifespan.shutdown.complete"})
                return

    path = scope["path"].split("/")
    client_id = int(path[-2])

    async with pool.acquire() as conn:
        if path[-1] == "transacoes":
            body = await read_body(receive)
            body = orjson.loads(body)
            await trasaction(client_id, body, send, conn)
            return

        if path[-1] == "extrato":
            await statement(client_id, send, conn)
            return

    await send(
        {
            "type": "http.response.start",
            "status": 404,
            "headers": [
                [b"content-type", b"text/plain"],
            ],
        }
    )
    await send(
        {
            "type": "http.response.body",
            "body": b"",
        }
    )


if __name__ == "__main__":
    uvicorn.run(
        "uvicorn_raw:app",
        host="0.0.0.0",
        port=8000,
    )
