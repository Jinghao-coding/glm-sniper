import json
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

from dotenv import load_dotenv, dotenv_values

BEIJING_TZ = timezone(timedelta(hours=8))

PRODUCT_MAP = {
    "product-1df3e1": "Pro 包月 (¥149/月)",
    "product-fef82f": "Pro 包季 (¥134.1/月)",
    "product-5643e6": "Pro 包年 (¥119.2/月)",
    "product-02434c": "Lite 包月 (¥49/月)",
    "product-2fc421": "Max 包月 (¥469/月)",
}


def product_name(product_id: str) -> str:
    return PRODUCT_MAP.get(product_id, product_id)


def product_choices() -> list[dict]:
    choices = [{"id": k, "name": v} for k, v in PRODUCT_MAP.items()]
    choices.append({"id": "_custom", "name": "自定义 (手动输入)"})
    return choices


def beijing_now() -> datetime:
    return datetime.now(BEIJING_TZ)


def _env(env_dict, key, default=None, cast=None):
    val = env_dict.get(key, default)
    if val is None or val == "":
        if default is None:
            return None
        val = default
    if cast and val is not None:
        return cast(val)
    return val


def _load_from_os():
    load_dotenv()
    return dict(os.environ)


def load_config():
    env = _load_from_os()

    authorization = _env(env, "AUTHORIZATION")
    if not authorization or authorization == "your-jwt-token-here":
        print("错误: 请在 .env 中设置 AUTHORIZATION (JWT token)")
        sys.exit(1)

    product_id = _env(env, "PRODUCT_ID")
    if not product_id:
        print("错误: 请在 .env 中设置 PRODUCT_ID")
        sys.exit(1)

    invitation_code = _env(env, "INVITATION_CODE", "")
    preview_body = json.dumps(
        {"productId": product_id, "invitationCode": invitation_code}
    )

    cookie_str = _env(env, "COOKIE_STRING", "")

    return {
        "authorization": authorization,
        "product_id": product_id,
        "preview_body": preview_body,
        "cookie_str": cookie_str,
        "turbo_concurrency": _env(env, "TURBO_CONCURRENCY", "10", int),
        "normal_concurrency": _env(env, "NORMAL_CONCURRENCY", "5", int),
        "turbo_duration": _env(env, "TURBO_DURATION", "5", float),
        "max_retry": _env(env, "MAX_RETRY", "2000", int),
        "rush_time": _env(env, "RUSH_TIME", "10:00:00"),
        "preheat_before": _env(env, "PREHEAT_BEFORE", "3", int),
        "request_timeout": _env(env, "REQUEST_TIMEOUT", "10", int),
        "connection_pool_size": _env(env, "CONNECTION_POOL_SIZE", "50", int),
        "warmup_count": _env(env, "WARMUP_COUNT", "5", int),
        "play_sound": _env(env, "PLAY_SOUND", "true", lambda x: x.lower() == "true"),
        "desktop_notify": _env(env, "DESKTOP_NOTIFY", "true", lambda x: x.lower() == "true"),
    }


def config_from_account(account: dict) -> dict:
    """Build a config dict from a database account row."""
    product_id = account.get("product_id", "product-1df3e1")
    invitation_code = account.get("invitation_code", "")
    preview_body = json.dumps(
        {"productId": product_id, "invitationCode": invitation_code}
    )

    return {
        "authorization": account["authorization"],
        "product_id": product_id,
        "preview_body": preview_body,
        "cookie_str": account.get("cookie_string", ""),
        "turbo_concurrency": account.get("turbo_concurrency", 10),
        "normal_concurrency": account.get("normal_concurrency", 5),
        "turbo_duration": account.get("turbo_duration", 5.0),
        "max_retry": account.get("max_retry", 2000),
        "rush_time": account.get("rush_time", "10:00:00"),
        "preheat_before": account.get("preheat_before", 3),
        "request_timeout": account.get("request_timeout", 10),
        "connection_pool_size": account.get("connection_pool_size", 50),
        "warmup_count": account.get("warmup_count", 5),
        "play_sound": bool(account.get("play_sound", 1)),
        "desktop_notify": bool(account.get("desktop_notify", 1)),
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


def load_env_as_account() -> dict | None:
    env_path = Path(".") / ".env"
    if not env_path.exists():
        return None
    vals = dotenv_values(str(env_path))
    authorization = (vals.get("AUTHORIZATION") or "").strip().strip('"').strip("'")
    if not authorization or authorization == "your-jwt-token-here":
        return None
    product_id = (vals.get("PRODUCT_ID") or "").strip().strip('"').strip("'")
    if not product_id:
        return None
    return {
        "name": ".env 默认账号",
        "authorization": authorization,
        "product_id": product_id,
        "invitation_code": (vals.get("INVITATION_CODE") or "").strip().strip('"').strip("'"),
        "cookie_string": (vals.get("COOKIE_STRING") or "").strip().strip('"').strip("'"),
        "turbo_concurrency": int((vals.get("TURBO_CONCURRENCY") or "10").strip().strip('"')),
        "normal_concurrency": int((vals.get("NORMAL_CONCURRENCY") or "5").strip().strip('"')),
        "turbo_duration": float((vals.get("TURBO_DURATION") or "5").strip().strip('"')),
        "max_retry": int((vals.get("MAX_RETRY") or "2000").strip().strip('"')),
        "rush_time": (vals.get("RUSH_TIME") or "10:00:00").strip().strip('"'),
        "preheat_before": int((vals.get("PREHEAT_BEFORE") or "3").strip().strip('"')),
        "request_timeout": int((vals.get("REQUEST_TIMEOUT") or "10").strip().strip('"')),
        "connection_pool_size": int((vals.get("CONNECTION_POOL_SIZE") or "50").strip().strip('"')),
        "warmup_count": int((vals.get("WARMUP_COUNT") or "5").strip().strip('"')),
        "play_sound": (vals.get("PLAY_SOUND") or "true").strip().strip('"').lower() == "true",
        "desktop_notify": (vals.get("DESKTOP_NOTIFY") or "true").strip().strip('"').lower() == "true",
    }
