import os, asyncio
from datetime import datetime, timezone
from typing import Optional
import asyncpg
import httpx
from fastapi import FastAPI, Request, Header

DATABASE_URL        = os.environ["DATABASE_URL"]
EZR_BASE_URL        = os.environ["EZR_BASE_URL"]           # e.g. https://<tenant>.ezrentout.com/api/v3
EZR_API_KEY         = os.environ["EZR_API_KEY"]            # EZRentOut API token
POLL_INTERVAL_SECS  = int(os.getenv("POLL_INTERVAL_SECS", "60"))
ENABLE_LOOP         = os.getenv("ENABLE_INTERNAL_POLL_LOOP", "1") == "1"

app = FastAPI()

async def upsert_task(evt: dict):
    q = """
    insert into public.tasks (source, external_id, title, status, start_at, customer, updated_at)
    values ($1,$2,$3,$4,$5,$6,$7)
    on conflict (source, external_id)
    do update set title=excluded.title,
                  status=excluded.status,
                  start_at=excluded.start_at,
                  customer=excluded.customer,
                  updated_at=excluded.updated_at
    """
    # create a pool for just this operation; avoids crash at startup
    async with asyncpg.create_pool(DATABASE_URL) as pool:
        async with pool.acquire() as con:
            await con.execute(
                q,
                evt["source"],
                evt["external_id"],
                evt["title"],
                evt["status"],
                evt.get("start_at"),
                evt.get("customer"),
                evt.get("updated_at") or datetime.now(timezone.utc),
            )

@app.get("/health")
async def health():
    return {"ok": True}

# ---------- DRS WEBHOOK ----------
@app.post("/webhooks/drs")
async def drs_webhook(req: Request, x_signature: str | None = Header(default=None)):
    body = await req.json()
    job = body.get("job") or body
    evt = {
        "source": "drs",
        "external_id": str(job["id"]),
        "title": job.get("title", f'Job #{job["id"]}'),
        "status": job.get("state", "Scheduled"),
        "start_at": job.get("scheduled_at"),
        "customer": job.get("customer_name"),
        "updated_at": datetime.now(timezone.utc),
    }
    await upsert_task(evt)
    return {"ok": True}

# ---------- EZRentOut POLLING ----------
async def fetch_ezr_page(client, cursor: Optional[str]):
    # Adjust params to your EZR API version if needed
    params = {}
    if cursor:
        params["updated_after"] = cursor
    headers = {"Authorization": f"Bearer {EZR_API_KEY}"}
    r = await client.get(f"{EZR_BASE_URL}/orders", headers=headers, params=params, timeout=30.0)
    r.raise_for_status()
    return r.json()

async def ezr_to_event(order: dict) -> dict:
    return {
        "source": "ezrentout",
        "external_id": str(order["id"]),
        "title": f'Order #{order["id"]} – {order.get("title", "Rental")}',
        "status": order.get("status", "Booked"),
        "start_at": order.get("start_time"),
        "customer": (order.get("customer") or {}).get("name"),
        "updated_at": datetime.now(timezone.utc),
    }

async def poll_ezr_once():
    # load cursor
    async with asyncpg.create_pool(DATABASE_URL) as pool:
        async with pool.acquire() as con:
            row = await con.fetchrow("select last_cursor from public.sync_state where source='ezrentout'")
            cursor = row["last_cursor"] if row else None

    # fetch page
    async with httpx.AsyncClient() as client:
        data = await fetch_ezr_page(client, cursor)

    # map orders
    orders = data.get("data") or data.get("results") or data
    if isinstance(orders, dict):
        orders = orders.get("orders", [])

    # upsert each
    for o in orders:
        await upsert_task(await ezr_to_event(o))

    # save next cursor
    next_cursor = data.get("next_cursor")
    if not next_cursor:
        updated_vals = [o.get("updated_at") or o.get("modified_at") for o in orders if (o.get("updated_at") or o.get("modified_at"))]
        if updated_vals:
            next_cursor = max(updated_vals)

    if next_cursor:
        async with asyncpg.create_pool(DATABASE_URL) as pool:
            async with pool.acquire() as con:
                await con.execute("""
                    insert into public.sync_state(source, last_cursor)
                    values ('ezrentout', $1)
                    on conflict (source) do update set last_cursor=excluded.last_cursor
                """, next_cursor)

@app.post("/tasks/poll/ezrentout")
async def poll_now():
    await poll_ezr_once()
    return {"ok": True}

@app.on_event("startup")
async def start_loop():
    if not ENABLE_LOOP:
        return
    async def loop():
        await asyncio.sleep(3)
        while True:
            try:
                await poll_ezr_once()
            except Exception as e:
                print("EZR poll error:", e)
            await asyncio.sleep(POLL_INTERVAL_SECS)
    asyncio.create_task(loop())
