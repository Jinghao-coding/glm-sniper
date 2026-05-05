import asyncio
import json
import time
from contextlib import asynccontextmanager

import aiohttp
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path

from rush_engine.config import config_from_account, parse_cookies, load_env_as_account, product_name, product_choices
from rush_engine.database import (
    create_account, list_accounts, get_account, update_account, delete_account,
    create_session, update_session, list_sessions, get_session,
    list_attempts, create_attempt,
)
from rush_engine.runner import concurrent_rush, wait_until_rush_time
from rush_engine.stats import RushStats
from rush_engine.time_sync import sync_time

STATIC_DIR = Path(__file__).parent / "static"

active_sessions: dict[str, asyncio.Task] = {}
active_stop_flags: dict[str, asyncio.Event] = {}
active_stats: dict[str, RushStats] = {}
active_event_queues: dict[str, asyncio.Queue] = {}
ws_connections: list[WebSocket] = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    for sid, flag in active_stop_flags.items():
        flag.set()
    for sid, task in active_sessions.items():
        task.cancel()


app = FastAPI(title="GLM Sniper", lifespan=lifespan)


async def _broadcast(event: dict):
    msg = json.dumps(event, ensure_ascii=False, default=str)
    dead = []
    for ws in ws_connections:
        try:
            await ws.send_text(msg)
        except Exception:
            dead.append(ws)
    for ws in dead:
        ws_connections.remove(ws)


async def _event_forwarder(event_queue: asyncio.Queue, session_id: str):
    while True:
        try:
            event = await asyncio.wait_for(event_queue.get(), timeout=30)
        except asyncio.TimeoutError:
            continue
        await _broadcast(event)

        if event.get("type") == "batch_result":
            stats = event.get("stats", {})
            update_session(session_id, {
                "total_attempts": stats.get("total", 0),
                "success_count": stats.get("successes", 0),
                "error_count": stats.get("errors", 0),
                "elapsed_ms": stats.get("elapsed_ms", 0),
            })
            reasons = event.get("reasons", [])
            if reasons:
                create_attempt(
                    session_id=session_id,
                    attempt_num=event.get("attempt", 0),
                    ok=False,
                    reason=reasons[0],
                )

        elif event.get("type") == "rush_success":
            stats = event.get("stats", {})
            update_session(session_id, {
                "status": "success",
                "finished_at": _now_iso(),
                "total_attempts": stats.get("total", 0),
                "success_count": stats.get("successes", 0),
                "error_count": stats.get("errors", 0),
                "elapsed_ms": stats.get("elapsed_ms", 0),
                "result_biz_id": event.get("biz_id"),
            })
            create_attempt(
                session_id=session_id,
                attempt_num=event.get("attempt", 0),
                ok=True,
                biz_id=event.get("biz_id"),
            )

        elif event.get("type") in ("auth_expired", "captcha_required"):
            update_session(session_id, {
                "status": "failed",
                "finished_at": _now_iso(),
            })

        elif event.get("type") == "rush_end":
            reason = event.get("reason", "unknown")
            stats = event.get("stats", {})
            update_session(session_id, {
                "status": "failed" if reason != "stopped" else "stopped",
                "finished_at": _now_iso(),
                "total_attempts": stats.get("total", 0),
                "success_count": stats.get("successes", 0),
                "error_count": stats.get("errors", 0),
                "elapsed_ms": stats.get("elapsed_ms", 0),
            })


def _now_iso() -> str:
    from datetime import datetime, timezone, timedelta
    return datetime.now(timezone(timedelta(hours=8))).isoformat()


async def _run_rush(session_id: str, account_id: str):
    account = get_account(account_id)
    if not account:
        update_session(session_id, {"status": "failed", "finished_at": _now_iso()})
        return

    config = config_from_account(account)
    event_queue = asyncio.Queue(maxsize=1000)
    active_event_queues[session_id] = event_queue
    stats = RushStats()
    active_stats[session_id] = stats
    stop_flag = asyncio.Event()
    active_stop_flags[session_id] = stop_flag

    update_session(session_id, {"status": "running", "started_at": _now_iso()})

    forwarder_task = asyncio.create_task(_event_forwarder(event_queue, session_id))

    try:
        cookies = parse_cookies(config["cookie_str"])
        conn = aiohttp.TCPConnector(
            limit=config["connection_pool_size"],
            limit_per_host=config["connection_pool_size"],
            ttl_dns_cache=300,
        )
        timeout = aiohttp.ClientTimeout(total=config["request_timeout"])
        headers = {
            "Authorization": config["authorization"],
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
        }

        async with aiohttp.ClientSession(
            cookies=cookies, connector=conn, timeout=timeout, headers=headers
        ) as http_session:
            offset_ms = await sync_time(http_session)
            await wait_until_rush_time(
                http_session, config, offset_ms, stats, stop_flag,
                event_queue=event_queue, session_id=session_id,
            )

            if stop_flag.is_set():
                update_session(session_id, {"status": "stopped", "finished_at": _now_iso()})
                return

            result = await concurrent_rush(
                http_session, config, stats, stop_flag,
                event_queue=event_queue, session_id=session_id,
            )

            if result and result.get("ok"):
                update_session(session_id, {
                    "status": "success",
                    "finished_at": _now_iso(),
                    "result_biz_id": result.get("bizId"),
                    "result_data": json.dumps(result, ensure_ascii=False),
                })
    except asyncio.CancelledError:
        update_session(session_id, {"status": "stopped", "finished_at": _now_iso()})
    except Exception as e:
        update_session(session_id, {"status": "failed", "finished_at": _now_iso()})
        await _broadcast({
            "type": "error",
            "session_id": session_id,
            "message": str(e),
        })
    finally:
        forwarder_task.cancel()
        active_sessions.pop(session_id, None)
        active_stop_flags.pop(session_id, None)
        active_stats.pop(session_id, None)
        active_event_queues.pop(session_id, None)


# ─── WebSocket ───

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    ws_connections.append(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        if ws in ws_connections:
            ws_connections.remove(ws)


# ─── Product API ───

@app.get("/api/products")
async def api_products():
    return product_choices()


@app.get("/api/products/{product_id}/name")
async def api_product_name(product_id: str):
    return {"id": product_id, "name": product_name(product_id)}


# ─── .env Import API ───

@app.post("/api/env/import")
async def api_env_import():
    data = load_env_as_account()
    if not data:
        return {"error": ".env 文件不存在或缺少 AUTHORIZATION/PRODUCT_ID"}
    existing = list_accounts()
    for acc in existing:
        if acc.get("authorization") == data["authorization"] and acc.get("product_id") == data["product_id"]:
            return {"info": "该账号已存在", "account": acc}
    account = create_account(data)
    return {"ok": True, "account": account}


@app.get("/api/env/check")
async def api_env_check():
    data = load_env_as_account()
    if not data:
        return {"available": False, "reason": ".env 文件不存在或缺少必要配置"}
    return {"available": True, "name": data["name"], "product_id": data["product_id"], "product_name": product_name(data["product_id"])}


# ─── Account API ───

@app.get("/api/accounts")
async def api_list_accounts():
    return list_accounts()


@app.post("/api/accounts")
async def api_create_account(data: dict):
    if not data.get("authorization"):
        return {"error": "authorization is required"}
    return create_account(data)


@app.get("/api/accounts/{account_id}")
async def api_get_account(account_id: str):
    account = get_account(account_id)
    if not account:
        return {"error": "not found"}
    return account


@app.put("/api/accounts/{account_id}")
async def api_update_account(account_id: str, data: dict):
    result = update_account(account_id, data)
    if not result:
        return {"error": "not found"}
    return result


@app.delete("/api/accounts/{account_id}")
async def api_delete_account(account_id: str):
    if delete_account(account_id):
        return {"ok": True}
    return {"error": "not found"}


# ─── Session API ───

@app.get("/api/sessions")
async def api_list_sessions(account_id: str | None = None, limit: int = 50):
    return list_sessions(account_id=account_id, limit=limit)


@app.get("/api/sessions/{session_id}")
async def api_get_session(session_id: str):
    session = get_session(session_id)
    if not session:
        return {"error": "not found"}
    return session


@app.get("/api/sessions/{session_id}/attempts")
async def api_list_attempts(session_id: str, limit: int = 200):
    return list_attempts(session_id, limit=limit)


# ─── Rush Control API ───

@app.post("/api/rush/start")
async def api_rush_start(data: dict):
    account_id = data.get("account_id")
    if not account_id:
        return {"error": "account_id is required"}
    account = get_account(account_id)
    if not account:
        return {"error": "account not found"}

    session = create_session(account_id)
    session_id = session["id"]

    task = asyncio.create_task(_run_rush(session_id, account_id))
    active_sessions[session_id] = task

    return {"session_id": session_id, "status": "started"}


@app.post("/api/rush/stop")
async def api_rush_stop(data: dict):
    session_id = data.get("session_id")
    if not session_id:
        return {"error": "session_id is required"}

    flag = active_stop_flags.get(session_id)
    if flag:
        flag.set()
        return {"status": "stopping"}
    return {"error": "session not found or already stopped"}


@app.get("/api/rush/status")
async def api_rush_status():
    result = {}
    for sid, stats in active_stats.items():
        result[sid] = stats.to_dict()
    return result


# ─── Frontend ───

@app.get("/", response_class=HTMLResponse)
async def index():
    html_file = STATIC_DIR / "index.html"
    return HTMLResponse(html_file.read_text(encoding="utf-8"))


app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
