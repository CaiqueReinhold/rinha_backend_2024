import asyncpg


async def create_transaction(
    conn: asyncpg.Connection, client_id: int, value: int, t_type: str, description: str
) -> dict:
    async with conn.transaction():
        await conn.execute(
            "INSERT INTO transactions (client_id, value, type, description) "
            "VALUES ($1, $2, $3, $4)",
            client_id,
            value,
            t_type,
            description,
        )
        row = await conn.fetchrow(
            "UPDATE clients "
            "SET balance = balance + $2 "
            "WHERE client_id = $1 "
            'RETURNING "limit", "balance"',
            client_id,
            value if t_type == "c" else -value,
        )
        if row is None:
            raise KeyError("Client not found")

        if t_type == "d" and row["balance"] + row["limit"] < 0:
            raise ValueError("Transaction value exceeds client limit")

    return {
        "saldo": row["balance"],
        "limite": row["limit"],
    }


async def get_statement(conn: asyncpg.Connection, client_id: int):
    c = await conn.fetchrow(
        'SELECT "balance" AS total, "limit" as limite, '
        "trim(both '\"' from to_json(now())::text)::text AS data_extrato "
        "FROM clients WHERE client_id = $1",
        client_id,
    )
    if c is None:
        raise KeyError("Client not found")
    transactions = await conn.fetch(
        "SELECT value AS valor, type AS tipo, description AS descricao, "
        'trim(both \'"\' from to_json("date")::text)::text AS realizada_em '
        "FROM transactions WHERE client_id = $1 "
        "ORDER BY date DESC "
        "LIMIT 10",
        client_id,
    )
    return {"saldo": dict(c), "ultimas_transacoes": [dict(t) for t in transactions]}
