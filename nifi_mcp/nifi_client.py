"""
Authentication and the read-only HTTP layer for talking to NiFi.

Only GET requests are allowed against NiFi resources (the single exception is the
POST to /access/token needed to authenticate). This is where the "read-only"
guarantee is enforced at the code level, and where the username/password are used
and then never exposed again.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Optional

import httpx

from . import config

_token: Optional[str] = None
_token_exp: float = 0.0
_auth_lock = asyncio.Lock()


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(base_url=config.BASE_URL, verify=config.VERIFY_SSL, timeout=30.0)


async def _login() -> str:
    """Obtain a JWT using single-user credentials. Username/password stay here."""
    global _token, _token_exp
    if not (config.BASE_URL and config.USERNAME and config.PASSWORD):
        raise RuntimeError(
            "NIFI_BASE_URL / NIFI_USERNAME / NIFI_PASSWORD environment variables are not set."
        )
    async with _client() as c:
        # The ONLY POST permitted, and only for authentication.
        resp = await c.post(
            f"{config.API}/access/token",
            data={"username": config.USERNAME, "password": config.PASSWORD},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"NiFi authentication failed (HTTP {resp.status_code}).")
    token = resp.text.strip()
    if not token:
        raise RuntimeError("NiFi returned an empty token.")
    _token = token
    _token_exp = time.time() + 7 * 3600
    return token


async def _get_token() -> str:
    async with _auth_lock:
        if _token and time.time() < _token_exp:
            return _token
        return await _login()


async def _request(method: str, path: str, params: Optional[dict] = None) -> Any:
    """
    Read-only HTTP layer. Any method other than GET is REJECTED at the code level,
    so the server can never write/delete even if extended carelessly later.
    """
    if method.upper() != "GET":
        raise PermissionError("Read-only server: only GET is allowed. Write/delete rejected.")
    token = await _get_token()
    async with _client() as c:
        resp = await c.get(path, params=params, headers={"Authorization": f"Bearer {token}"})
    if resp.status_code == 401:
        await _login()
        token = await _get_token()
        async with _client() as c:
            resp = await c.get(path, params=params, headers={"Authorization": f"Bearer {token}"})
    resp.raise_for_status()
    if resp.headers.get("content-type", "").startswith("application/json"):
        return resp.json()
    return resp.text


async def get(path: str, params: Optional[dict] = None) -> Any:
    """Convenience GET wrapper used across the codebase."""
    return await _request("GET", path, params)
