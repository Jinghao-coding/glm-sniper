#!/usr/bin/env python3
"""
GLM Coding Plan 抢购脚本 - CLI 入口

Usage:
  1. cp .env.example .env
  2. 编辑 .env: 填入 AUTHORIZATION 和 PRODUCT_ID
  3. uv venv && source .venv/bin/activate && uv pip install -r requirements.txt
  4. python glm_sniper.py
"""

import asyncio
import json
import sys
from pathlib import Path

import aiohttp

from rush_engine.config import load_config, parse_cookies
from rush_engine.runner import (
    concurrent_rush,
    wait_until_rush_time,
    send_notification,
    CHECK_URL,
    _RICH,
    _console,
)
from rush_engine.stats import RushStats
from rush_engine.time_sync import sync_time

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
