import asyncio
import json
import random
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import aiohttp

from .config import BEIJING_TZ
from .stats import RushStats
from .time_sync import sync_time

PREVIEW_URL = "https://bigmodel.cn/api/biz/pay/preview"
CHECK_URL = "https://bigmodel.cn/api/biz/pay/check"

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich import box

    _RICH = True
    _console = Console()
except ImportError:
    _RICH = False
    _console = None


def _emit_event(event_queue: asyncio.Queue | None, event: dict):
    if event_queue is None:
        return
    try:
        event_queue.put_nowait(event)
    except asyncio.QueueFull:
        pass


async def single_attempt(
    session: aiohttp.ClientSession,
    url: str,
    body: str,
    auth: str,
    attempt_num: int,
    timeout: int,
) -> dict:
    req_headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "Authorization": auth,
        "Accept": "application/json, text/plain, */*",
        "X-Request-Id": hex(int(time.time() * 1000000))[-12:],
        "X-Timestamp": str(int(time.time() * 1000)),
        "Accept-Language": f"zh-CN,zh;q={0.5 + random.random() * 0.5:.1f}",
    }

    try:
        async with session.post(
            url,
            data=body,
            headers=req_headers,
            timeout=aiohttp.ClientTimeout(total=timeout),
        ) as resp:
            status = resp.status
            text = await resp.text()

            if status in (401, 403):
                return {
                    "ok": False,
                    "reason": f"HTTP {status} 会话过期",
                    "attempt": attempt_num,
                }
            if status == 429:
                return {"ok": False, "reason": "429 限流", "attempt": attempt_num}

            try:
                data = json.loads(text)
            except json.JSONDecodeError:
                return {"ok": False, "reason": "非JSON响应", "attempt": attempt_num}

            if data.get("code") == 200 and data.get("data", {}).get("bizId"):
                biz_id = data["data"]["bizId"]
                try:
                    async with session.get(
                        f"{CHECK_URL}?bizId={biz_id}",
                        headers={"Authorization": auth},
                        timeout=aiohttp.ClientTimeout(total=timeout),
                    ) as check_resp:
                        check_text = await check_resp.text()
                        try:
                            check_data = json.loads(check_text)
                        except json.JSONDecodeError:
                            check_data = {}

                        if check_data.get("data") == "EXPIRE":
                            return {
                                "ok": False,
                                "reason": "EXPIRE",
                                "attempt": attempt_num,
                            }

                        return {
                            "ok": True,
                            "bizId": biz_id,
                            "data": data["data"],
                            "text": text,
                            "attempt": attempt_num,
                        }
                except Exception as e:
                    return {
                        "ok": False,
                        "reason": f"check异常: {e}",
                        "attempt": attempt_num,
                    }

            code = data.get("code", -1)
            if code == 500 and "安全验证" in data.get("msg", ""):
                reason = "需要安全验证"
            elif code == 555:
                reason = "系统繁忙"
            elif data.get("data", {}).get("bizId") is None:
                reason = "售罄"
            else:
                reason = f"code={code}"

            return {"ok": False, "reason": reason, "attempt": attempt_num}

    except asyncio.TimeoutError:
        return {"ok": False, "reason": "超时", "attempt": attempt_num}
    except aiohttp.ClientError as e:
        return {"ok": False, "reason": f"网络: {e}", "attempt": attempt_num}
    except Exception as e:
        return {"ok": False, "reason": f"异常: {e}", "attempt": attempt_num}


async def concurrent_rush(
    session: aiohttp.ClientSession,
    config: dict,
    stats: RushStats,
    stop_flag: asyncio.Event,
    event_queue: asyncio.Queue | None = None,
    session_id: str | None = None,
) -> dict | None:
    turbo_sec = config["turbo_duration"]
    turbo_n = config["turbo_concurrency"]
    normal_n = config["normal_concurrency"]
    max_retry = config["max_retry"]
    timeout = config["request_timeout"]
    body = config["preview_body"]
    auth = config["authorization"]

    stats.start_time = time.time() * 1000

    total_attempt = 0
    throttle_count = 0
    consecutive_sold_out = 0
    consecutive_network_err = 0

    _emit_event(event_queue, {
        "type": "rush_start",
        "session_id": session_id,
        "timestamp": time.time(),
        "config": {k: v for k, v in config.items() if k != "authorization"},
    })

    while total_attempt < max_retry and not stop_flag.is_set():
        elapsed = stats.elapsed_ms
        is_turbo = elapsed < turbo_sec * 1000
        cur_concurrency = turbo_n if is_turbo else normal_n
        batch_size = min(cur_concurrency, max_retry - total_attempt)

        tasks = []
        for _ in range(batch_size):
            total_attempt += 1
            tasks.append(
                single_attempt(session, PREVIEW_URL, body, auth, total_attempt, timeout)
            )
            stats.total = total_attempt

        results = await asyncio.gather(*tasks)

        winner = next((r for r in results if r.get("ok")), None)
        if winner:
            stats.successes += 1
            stats.last_bizid = winner.get("bizId")
            stats.last_response = winner
            _emit_event(event_queue, {
                "type": "rush_success",
                "session_id": session_id,
                "timestamp": time.time(),
                "biz_id": winner.get("bizId"),
                "attempt": winner.get("attempt"),
                "stats": stats.to_dict(),
            })
            return winner

        failed = [r for r in results if not r.get("ok")]
        reasons = [r.get("reason", "未知") for r in failed]
        stats.errors += sum(1 for r in failed if "网络" not in r.get("reason", ""))

        _emit_event(event_queue, {
            "type": "batch_result",
            "session_id": session_id,
            "timestamp": time.time(),
            "attempt": total_attempt,
            "batch_size": batch_size,
            "reasons": reasons,
            "is_turbo": is_turbo,
            "stats": stats.to_dict(),
        })

        if any("会话过期" in r for r in reasons):
            _emit_event(event_queue, {
                "type": "auth_expired",
                "session_id": session_id,
                "timestamp": time.time(),
            })
            if _RICH:
                _console.print(
                    "[bold red]会话已过期, 请重新获取 Authorization Token![/bold red]"
                )
            else:
                print("会话已过期, 请重新获取 Authorization Token!")
            return None

        if any("安全验证" in r for r in reasons):
            _emit_event(event_queue, {
                "type": "captcha_required",
                "session_id": session_id,
                "timestamp": time.time(),
            })
            if _RICH:
                _console.print(
                    "[bold red]需要安全验证(CAPTCHA)![/bold red]\n"
                    "[yellow]请先在浏览器中打开 bigmodel.cn 完成一次购买流程的安全验证[/yellow]"
                )
            else:
                print("需要安全验证(CAPTCHA)! 请先在浏览器中完成一次购买流程的安全验证")
            return None

        if any("429" in r or "限流" in r for r in reasons):
            throttle_count += 1
            backoff = min(2000 * (2 ** min(throttle_count, 4)), 16000)
            _emit_event(event_queue, {
                "type": "throttled",
                "session_id": session_id,
                "timestamp": time.time(),
                "backoff_ms": backoff,
            })
            if _RICH:
                _console.print(f"[yellow]限流, 退避 {backoff}ms[/yellow]")
            else:
                print(f"限流, 退避 {backoff}ms")
            await asyncio.sleep(backoff / 1000)
            continue
        else:
            throttle_count = 0

        if all(r == "EXPIRE" for r in reasons):
            continue

        net_errors = sum(1 for r in reasons if r.startswith("网络"))
        if net_errors == batch_size:
            consecutive_network_err += 1
        else:
            consecutive_network_err = 0

        if consecutive_network_err >= 3:
            _emit_event(event_queue, {
                "type": "network_error",
                "session_id": session_id,
                "timestamp": time.time(),
            })
            if _RICH:
                _console.print("[yellow]网络异常, 暂停 3 秒...[/yellow]")
            else:
                print("网络异常, 暂停 3 秒...")
            await asyncio.sleep(3)
            consecutive_network_err = 0
            continue

        elapsed_sec = elapsed / 1000
        if elapsed_sec > 20:
            sold_out_count = sum(1 for r in reasons if r == "售罄")
            if sold_out_count == batch_size:
                consecutive_sold_out += 1
            else:
                consecutive_sold_out = 0

            if consecutive_sold_out >= 10:
                if _RICH:
                    _console.print("[dim]连续售罄, 降速 (2s)...[/dim]")
                else:
                    print(f"连续售罄, 降速 (2s)... 第 {total_attempt} 次")
                await asyncio.sleep(2)
                continue

        if (
            total_attempt <= 5 * cur_concurrency
            or total_attempt % (20 * cur_concurrency) == 0
        ):
            mode = "极速" if is_turbo else "普通"
            if _RICH:
                _console.print(
                    f"[dim]#{total_attempt} [{mode} {cur_concurrency}路] {reasons[0]} "
                    f"({elapsed_sec:.1f}s)[/dim]"
                )
            else:
                rps = stats.rate
                print(
                    f"#{total_attempt} [{mode} {cur_concurrency}路] {reasons[0]} "
                    f"({elapsed_sec:.1f}s, {rps:.0f} req/s)"
                )

        batch_num = total_attempt // cur_concurrency
        if batch_num <= 20:
            delay = 0
        elif batch_num <= 80:
            delay = (15 + random.random() * 30) / 1000
        else:
            delay = (50 + random.random() * 80) / 1000

        if delay > 0:
            await asyncio.sleep(delay)

    _emit_event(event_queue, {
        "type": "rush_end",
        "session_id": session_id,
        "timestamp": time.time(),
        "stats": stats.to_dict(),
        "reason": "max_retry" if total_attempt >= max_retry else "stopped",
    })
    return None


async def warmup(session: aiohttp.ClientSession, config: dict) -> None:
    count = config["warmup_count"]
    auth = config["authorization"]
    if _RICH:
        _console.print(f"[dim]🔥 预热中 ({count} 次连接)...[/dim]")
    else:
        print(f"预热中 ({count} 次连接)...")

    for i in range(count):
        try:
            async with session.get(
                f"{CHECK_URL}?bizId=warmup_{i}",
                headers={"Authorization": auth},
                timeout=aiohttp.ClientTimeout(total=3),
            ):
                pass
        except Exception:
            pass
        await asyncio.sleep(0.05)

    try:
        async with session.head(
            PREVIEW_URL,
            headers={"Authorization": auth},
            timeout=aiohttp.ClientTimeout(total=3),
        ):
            pass
    except Exception:
        pass

    if _RICH:
        _console.print("[dim]✅ 预热完成[/dim]\n")
    else:
        print("预热完成\n")


def send_notification(biz_id: str, config: dict) -> None:
    if config.get("desktop_notify") and sys.platform == "darwin":
        try:
            subprocess.run(
                [
                    "osascript",
                    "-e",
                    f'display notification "{biz_id}" with title "GLM 抢购成功!" sound name "Glass"',
                ],
                capture_output=True,
            )
        except Exception:
            pass

    if config.get("play_sound"):
        print("\a" * 3)

    if _RICH:
        _console.print(
            Panel.fit(
                f"[bold white on green] 抢购成功! [/bold white on green]\n\n"
                f"[bold]bizId:[/bold] {biz_id}\n\n"
                "请立即前往 https://bigmodel.cn 完成支付!",
                title="🎉 SUCCESS",
                border_style="green",
            )
        )
    else:
        print("\n" + "=" * 60)
        print("  🎉 抢购成功!")
        print(f"  bizId: {biz_id}")
        print("  请立即前往 https://bigmodel.cn 完成支付!")
        print("=" * 60 + "\n")


async def wait_until_rush_time(
    session: aiohttp.ClientSession,
    config: dict,
    offset_ms: float,
    stats: RushStats,
    stop_flag: asyncio.Event,
    event_queue: asyncio.Queue | None = None,
    session_id: str | None = None,
) -> None:
    parts = config["rush_time"].split(":")
    target_h, target_m = int(parts[0]), int(parts[1])
    target_s = int(parts[2]) if len(parts) > 2 else 0

    preheat_done = False

    while not stop_flag.is_set():
        server_now = time.time() * 1000 + offset_ms
        bj_now = datetime.fromtimestamp(server_now / 1000, tz=BEIJING_TZ)
        target = bj_now.replace(
            hour=target_h, minute=target_m, second=target_s, microsecond=0
        )
        if target.timestamp() * 1000 <= server_now:
            remaining = target.timestamp() * 1000 - server_now
            if remaining > -30000:
                break
            if _RICH:
                _console.print(f"[yellow]今天 {config['rush_time']} 已过[/yellow]")
            else:
                print(f"今天 {config['rush_time']} 已过")
            return

        remaining_ms = target.timestamp() * 1000 - server_now
        remaining_s = remaining_ms / 1000

        if not preheat_done and remaining_s <= config["preheat_before"] + 1:
            preheat_done = True
            await warmup(session, config)

        if remaining_s <= 60 and remaining_s > 0:
            _emit_event(event_queue, {
                "type": "countdown",
                "session_id": session_id,
                "timestamp": time.time(),
                "remaining_s": round(remaining_s, 1),
            })
            if not _RICH:
                clear = "\r" + " " * 40 + "\r"
                print(f"{clear}⏳ 距抢购还有 {remaining_s:.1f}s  ", end="", flush=True)

        if remaining_ms <= 0:
            break
        elif remaining_ms <= 100:
            while time.time() * 1000 + offset_ms < target.timestamp() * 1000:
                pass
            break
        elif remaining_ms <= 500:
            await asyncio.sleep(remaining_ms / 1000 - 0.002)
        elif remaining_ms <= 2000:
            await asyncio.sleep(0.1)
        else:
            await asyncio.sleep(min(1, remaining_ms / 1000 - 0.5))
