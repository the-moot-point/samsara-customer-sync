from __future__ import annotations

import json
import logging
import os
import random
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

LOG = logging.getLogger(__name__)


def _utc_ts() -> str:
    import datetime
    return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()


@dataclass
class RetryConfig:
    max_attempts: int = 8
    base_delay: float = 0.5  # seconds
    max_delay: float = 30.0  # seconds


class SamsaraClient:
    """
    Thin API client with automatic retries/backoff and pagination helpers.
    Only allowed endpoints are used per requirements.
    """
    def __init__(
        self,
        api_token: str | None = None,
        base_url: str = "https://api.samsara.com",
        retry: RetryConfig | None = None,
        min_interval: float = 0.0,  # optional client-side throttle between calls
        timeout: float = 30.0,
    ) -> None:
        token = api_token or os.getenv("SAMSARA_API_TOKEN")
        if not token:
            raise RuntimeError("SAMSARA_API_TOKEN is required")
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "encompass-to-samsara/0.1",
            }
        )
        self.retry = retry or RetryConfig()
        self.min_interval = min_interval
        self.timeout = timeout
        self._last_call = 0.0

    # --------------- Core HTTP ---------------

    def _sleep_for_rate(self) -> None:
        if self.min_interval <= 0:
            return
        now = time.time()
        delta = now - self._last_call
        if delta < self.min_interval:
            time.sleep(self.min_interval - delta)

    def request(self, method: str, path: str, *, params: dict | None = None, json_body: Any | None = None) -> requests.Response:
        url = f"{self.base_url}{path}"
        attempt = 0
        delay = self.retry.base_delay
        while True:
            attempt += 1
            self._sleep_for_rate()
            try:
                resp = self.session.request(method, url, params=params, json=json_body, timeout=self.timeout)
            except requests.RequestException as e:
                if attempt >= self.retry.max_attempts:
                    LOG.error("HTTP error after %s attempts: %s", attempt, repr(e))
                    raise
                wait = min(self.retry.max_delay, delay * (2 ** (attempt - 1))) * (1 + random.random() * 0.25)
                LOG.warning("HTTP exception on %s %s (attempt %s), retrying in %.2fs", method, path, attempt, wait)
                time.sleep(wait)
                continue

            self._last_call = time.time()
            if resp.status_code in (429, 500, 502, 503, 504):
                if attempt >= self.retry.max_attempts:
                    LOG.error("HTTP %s after %s attempts: %s %s -> %s", resp.status_code, attempt, method, path, resp.text[:400])
                    resp.raise_for_status()
                retry_after = resp.headers.get("Retry-After")
                if retry_after is not None:
                    try:
                        wait = float(retry_after)
                    except ValueError:
                        wait = min(self.retry.max_delay, delay * (2 ** (attempt - 1)))
                else:
                    wait = min(self.retry.max_delay, delay * (2 ** (attempt - 1)))
                wait *= (1 + random.random() * 0.25)  # jitter
                LOG.warning("Rate/server error %s on %s %s (attempt %s), retrying in %.2fs",
                            resp.status_code, method, path, attempt, wait)
                time.sleep(wait)
                continue

            # success or permanent error
            if resp.status_code >= 400:
                LOG.error("HTTP error %s on %s %s: %s", resp.status_code, method, path, resp.text[:400])
            return resp

    # --------------- Endpoints ---------------

    def list_addresses(self, limit: int = 200) -> List[Dict[str, Any]]:
        """Iterate through all addresses (handling pagination if present)."""
        out: List[Dict[str, Any]] = []
        page_token = None
        while True:
            params = {"limit": limit}
            if page_token:
                params["pageToken"] = page_token
            r = self.request("GET", "/addresses", params=params)
            r.raise_for_status()
            data = r.json()
            items = data.get("data") or data.get("addresses") or data  # be permissive
            if isinstance(items, dict):
                # some APIs return {"addresses":[...]}
                items = items.get("addresses")
            if not isinstance(items, list):
                items = []
            out.extend(items)
            page_token = data.get("nextPageToken") or data.get("pagination", {}).get("nextPageToken")
            if not page_token:
                break
        return out

    def get_address(self, addr_id: str) -> Dict[str, Any]:
        r = self.request("GET", f"/addresses/{addr_id}")
        r.raise_for_status()
        return r.json()

    def create_address(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        r = self.request("POST", "/addresses", json_body=payload)
        r.raise_for_status()
        return r.json()

    def patch_address(self, addr_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        r = self.request("PATCH", f"/addresses/{addr_id}", json_body=payload)
        r.raise_for_status()
        return r.json()

    def delete_address(self, addr_id: str) -> None:
        r = self.request("DELETE", f"/addresses/{addr_id}")
        r.raise_for_status()

    def list_tags(self, limit: int = 200) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        page_token = None
        while True:
            params = {"limit": limit}
            if page_token:
                params["pageToken"] = page_token
            r = self.request("GET", "/tags", params=params)
            r.raise_for_status()
            data = r.json()
            items = data.get("data") or data.get("tags") or data
            if isinstance(items, dict):
                items = items.get("tags")
            if not isinstance(items, list):
                items = []
            out.extend(items)
            page_token = data.get("nextPageToken") or data.get("pagination", {}).get("nextPageToken")
            if not page_token:
                break
        return out
