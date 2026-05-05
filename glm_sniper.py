#!/usr/bin/env python3
"""
GLM Coding Plan 抢购脚本 - Async Concurrent Sniper

智谱开放平台 GLM Coding Plan 每日 10:00 (UTC+8) 限量释放库存。

Usage:
  1. cp .env.example .env
  2. 编辑 .env: 填入 AUTHORIZATION 和 PRODUCT_ID
  3. uv venv && source .venv/bin/activate && uv pip install -r requirements.txt
  4. python glm_sniper.py
"""

import asyncio
import json
import os
import random
import socket
import struct
import subprocess
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import aiohttp
from dotenv import load_dotenv

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich import box

    _RICH = True
    console = Console()
except ImportError:
    _RICH = False
    console = None


def load_config():
    load_dotenv()

    def env(key, default=None, cast=None):
        val = os.getenv(key, default)
        if val is None or val == "":
            if default is None:
                return None
            val = default
        if cast and val is not None:
            return cast(val)
        return val

    authorization = env("AUTHORIZATION")
    if not authorization or authorization == "your-jwt-token-here":
        print("错误: 请在 .env 中设置 AUTHORIZATION (JWT token)")
        sys.exit(1)

    product_id = env("PRODUCT_ID")
    if not product_id:
        print("错误: 请在 .env 中设置 PRODUCT_ID")
        sys.exit(1)

    invitation_code = env("INVITATION_CODE", "")
    preview_body = json.dumps(
        {"productId": product_id, "invitationCode": invitation_code}
    )

    cookie_str = env("COOKIE_STRING", "")

    return {
        "authorization": authorization,
        "product_id": product_id,
        "preview_body": preview_body,
        "cookie_str": cookie_str,
        "turbo_concurrency": env("TURBO_CONCURRENCY", "10", int),
        "normal_concurrency": env("NORMAL_CONCURRENCY", "5", int),
        "turbo_duration": env("TURBO_DURATION", "5", float),
        "max_retry": env("MAX_RETRY", "2000", int),
        "rush_time": env("RUSH_TIME", "10:00:00"),
        "preheat_before": env("PREHEAT_BEFORE", "3", int),
        "request_timeout": env("REQUEST_TIMEOUT", "10", int),
        "connection_pool_size": env("CONNECTION_POOL_SIZE", "50", int),
        "warmup_count": env("WARMUP_COUNT", "5", int),
        "play_sound": env("PLAY_SOUND", "true", lambda x: x.lower() == "true"),
        "desktop_notify": env("DESKTOP_NOTIFY", "true", lambda x: x.lower() == "true"),
    }


def parse_cookies(cookie_str: str) -> dict[str, str]:
    cookies = {}
    if not cookie_str:
        return cookies
    for item in cookie_str.split(";"):
        item = item.strip()
        if "=" in item:
            key, value = item.split("=", 1)
            cookies[key.strip()] = value.strip()
    return cookies


BEIJING_TZ = timezone(timedelta(hours=8))


def beijing_now() -> datetime:
    return datetime.now(BEIJING_TZ)


NTP_SERVERS = [
    "ntp.aliyun.com",
    "ntp.tencent.com",
    "time.apple.com",
    "ntp.tuna.tsinghua.edu.cn",
    "cn.ntp.org.cn",
]

NTP_PORT = 123
NTP_PACKET = b"\x1b" + b"\0" * 47
NTP_DELTA = 2208988800


def _ntp_request(host: str, timeout: float = 2.0) -> int | None:
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(timeout)
        sock.sendto(NTP_PACKET, (host, NTP_PORT))
        data, _ = sock.recvfrom(48)
        sock.close()
        t = struct.unpack("!12I", data)[10]
        if t == 0:
            return None
        return int((t - NTP_DELTA) * 1000)
    except Exception:
        return None


async def _http_time_sync(session: aiohttp.ClientSession) -> int | None:
    t0 = time.time() * 1000
    try:
        async with session.get(
            "https://bigmodel.cn/api/biz/pay/check?bizId=_sync",
            timeout=aiohttp.ClientTimeout(total=3),
        ) as resp:
            t1 = time.time() * 1000
            date_header = resp.headers.get("Date") or resp.headers.get("date")
            if date_header:
                server_ts = (
                    datetime.strptime(
                        date_header, "%a, %d %b %Y %H:%M:%S %Z"
                    ).timestamp()
                    * 1000
                )
                rtt = t1 - t0
                return int(server_ts + rtt / 2)
    except Exception:
        pass
    return None


async def sync_time(session: aiohttp.ClientSession) -> float:
    print("⏱  正在同步时间...")
    http_ts = await _http_time_sync(session)
    if http_ts:
        offset = http_ts - time.time() * 1000
        print(f"   HTTP 同步成功, 偏差: {offset:+.0f}ms")
        return offset
    for server in NTP_SERVERS:
        ntp_ts = _ntp_request(server)
        if ntp_ts:
            offset = ntp_ts - time.time() * 1000
            print(f"   NTP ({server}) 同步成功, 偏差: {offset:+.0f}ms")
            return offset
    offset = (datetime.now(BEIJING_TZ).timestamp() - time.time()) * 1000
    print(f"   ⚠  NTP 不可用，使用本地时钟, 偏差约: {offset:+.0f}ms")
    return offset


PREVIEW_URL = "https://bigmodel.cn/api/biz/pay/preview"
CHECK_URL = "https://bigmodel.cn/api/biz/pay/check"


class RushStats:
    def __init__(self):
        self.total = 0
        self.successes = 0
        self.errors = 0
        self.start_time = 0.0
        self.last_bizid = None
        self.last_response = None

    @property
    def elapsed_ms(self) -> float:
        return (time.time() * 1000 - self.start_time) if self.start_time else 0

    @property
    def rate(self) -> float:
        if self.elapsed_ms <= 0:
            return 0
        return self.total / (self.elapsed_ms / 1000)


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
            return winner

        failed = [r for r in results if not r.get("ok")]
        reasons = [r.get("reason", "未知") for r in failed]
        stats.errors += sum(1 for r in failed if "网络" not in r.get("reason", ""))

        if any("会话过期" in r for r in reasons):
            if _RICH:
                console.print(
                    "[bold red]会话已过期, 请重新获取 Authorization Token![/bold red]"
                )
            else:
                print("会话已过期, 请重新获取 Authorization Token!")
            return None

        if any("安全验证" in r for r in reasons):
            if _RICH:
                console.print(
                    "[bold red]需要安全验证(CAPTCHA)![/bold red]\n"
                    "[yellow]请先在浏览器中打开 bigmodel.cn 完成一次购买流程的安全验证[/yellow]"
                )
            else:
                print("需要安全验证(CAPTCHA)! 请先在浏览器中完成一次购买流程的安全验证")
            return None

        if any("429" in r or "限流" in r for r in reasons):
            throttle_count += 1
            backoff = min(2000 * (2 ** min(throttle_count, 4)), 16000)
            if _RICH:
                console.print(f"[yellow]限流, 退避 {backoff}ms[/yellow]")
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
            if _RICH:
                console.print("[yellow]网络异常, 暂停 3 秒...[/yellow]")
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
                    console.print("[dim]连续售罄, 降速 (2s)...[/dim]")
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
                console.print(
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

    return None


async def warmup(session: aiohttp.ClientSession, config: dict) -> None:
    count = config["warmup_count"]
    auth = config["authorization"]
    if _RICH:
        console.print(f"[dim]🔥 预热中 ({count} 次连接)...[/dim]")
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
        console.print("[dim]✅ 预热完成[/dim]\n")
    else:
        print("预热完成\n")


def send_notification(biz_id: str, config: dict) -> None:
    if config["desktop_notify"] and sys.platform == "darwin":
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

    if config["play_sound"]:
        print("\a" * 3)

    if _RICH:
        console.print(
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
                console.print(f"[yellow]今天 {config['rush_time']} 已过[/yellow]")
            else:
                print(f"今天 {config['rush_time']} 已过")
            return

        remaining_ms = target.timestamp() * 1000 - server_now
        remaining_s = remaining_ms / 1000

        if not preheat_done and remaining_s <= config["preheat_before"] + 1:
            preheat_done = True
            await warmup(session, config)

        if remaining_s <= 60 and remaining_s > 0:
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


async def main():
    config = load_config()

    if _RICH:
        console.print(
            Panel.fit(
                "[bold cyan]GLM Coding Plan 抢购脚本[/bold cyan]\n"
                f"目标套餐: [yellow]{config['product_id']}[/yellow]\n"
                f"抢购时间: [yellow]{config['rush_time']}[/yellow] (北京时间)\n"
                f"极速并发: [yellow]{config['turbo_concurrency']}[/yellow] 路 × {config['turbo_duration']}s\n"
                f"普通并发: [yellow]{config['normal_concurrency']}[/yellow] 路\n"
                f"最大重试: [yellow]{config['max_retry']}[/yellow] 次",
                title="⚡ GLM Sniper",
                border_style="cyan",
            )
        )
    else:
        print("=" * 60)
        print("  ⚡ GLM Coding Plan 抢购脚本")
        print(f"  目标套餐: {config['product_id']}")
        print(f"  抢购时间: {config['rush_time']} (北京时间)")
        print(
            f"  极速并发: {config['turbo_concurrency']} × {config['turbo_duration']}s"
        )
        print(f"  普通并发: {config['normal_concurrency']}")
        print(f"  最大重试: {config['max_retry']}")
        print("=" * 60)
        print()

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
        cookies=cookies,
        connector=conn,
        timeout=timeout,
        headers=headers,
    ) as session:
        offset_ms = await sync_time(session)

        if _RICH:
            console.print("[dim]🔍 验证认证状态...[/dim]")
        else:
            print("验证认证状态...")

        try:
            async with session.get(
                f"{CHECK_URL}?bizId=_validate",
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                if resp.status == 401 or resp.status == 403:
                    print("Authorization Token 已过期, 请重新获取!")
                    sys.exit(1)
                if _RICH:
                    console.print("[green]✅ 认证有效[/green]\n")
                else:
                    print("认证有效\n")
        except Exception as e:
            if _RICH:
                console.print(f"[yellow]⚠  无法验证认证状态: {e}[/yellow]\n")
            else:
                print(f"无法验证认证状态: {e}\n")

        stats = RushStats()
        stop_flag = asyncio.Event()

        def handle_signal():
            stop_flag.set()
            if _RICH:
                console.print("\n[yellow]收到中断信号, 正在停止...[/yellow]")
            else:
                print("\n收到中断信号, 正在停止...")

        try:
            loop = asyncio.get_running_loop()
            for sig in (2, 15):
                loop.add_signal_handler(sig, handle_signal)
        except NotImplementedError:
            pass

        await wait_until_rush_time(session, config, offset_ms, stats, stop_flag)

        if stop_flag.is_set():
            return

        if _RICH:
            console.print("\n[bold green]⚡ 抢购! 时间到![/bold green]")
        else:
            print("\n⚡ 抢购! 时间到!")

        result = await concurrent_rush(session, config, stats, stop_flag)

        if result and result.get("ok"):
            send_notification(result["bizId"], config)
            result_file = Path("rush_success.json")
            result_file.write_text(json.dumps(result, ensure_ascii=False, indent=2))
            print(f"成功响应已保存到 {result_file}")
        elif result:
            if _RICH:
                console.print(f"[red]抢购失败: {result.get('reason')}[/red]")
            else:
                print(f"抢购失败: {result.get('reason')}")
        else:
            if stop_flag.is_set():
                if _RICH:
                    console.print("[yellow]已手动停止[/yellow]")
                else:
                    print("已手动停止")
            else:
                if _RICH:
                    console.print("[red]达到最大重试次数, 抢购失败[/red]")
                else:
                    print("达到最大重试次数, 抢购失败")

        elapsed = stats.elapsed_ms / 1000
        if _RICH:
            table = Table(title="抢购统计", box=box.SIMPLE)
            table.add_column("指标", style="cyan")
            table.add_column("值", style="green")
            table.add_row("总请求数", str(stats.total))
            table.add_row("成功", str(stats.successes))
            table.add_row("错误", str(stats.errors))
            table.add_row("耗时", f"{elapsed:.2f}s")
            table.add_row("速率", f"{stats.rate:.0f} req/s")
            console.print(table)
        else:
            print(
                f"\n统计: 总 {stats.total} 次, 成功 {stats.successes}, "
                f"错误 {stats.errors}, 耗时 {elapsed:.2f}s, {stats.rate:.0f} req/s"
            )


if __name__ == "__main__":
    asyncio.run(main())
