import asyncio
import socket
import struct
import time
from datetime import datetime

import aiohttp

from .config import BEIJING_TZ

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
