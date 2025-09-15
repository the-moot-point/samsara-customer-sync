from __future__ import annotations

import logging
import os
import random
import time
from dataclasses import dataclass
from typing import Any, Literal

import requests

LOG = logging.getLogger(__name__)


def _utc_ts() -> str:
    import datetime

    return datetime.datetime.utcnow().replace(tzinfo=datetime.UTC).isoformat()


@dataclass
class RetryConfig:
    max_attempts: int = 8
    base_delay: float = 0.5  # seconds
    max_delay: float = 30.0  # seconds


class ExternalIdConflictError(requests.HTTPError):
    """Raised when a duplicate external ID is detected on update."""


class InvalidExternalIdKeyError(requests.HTTPError):
    """Raised when an external ID key violates Samsara key rules."""


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
        *,
        min_interval: float = 0.0,
        timeout: float = 30.0,
        rate_limits: dict[str, Any] | None = None,
    ) -> None:
        token = api_token or os.getenv("SAMSARA_BEARER_TOKEN")
        if not token:
            raise RuntimeError("SAMSARA_BEARER_TOKEN is required")
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
        # optional global throttle between any two requests
        self.min_interval = min_interval
        rate_limits = rate_limits or {}
        if "min_interval" in rate_limits:
            try:
                self.min_interval = float(rate_limits.pop("min_interval"))
            except (TypeError, ValueError):
                LOG.warning(
                    "Invalid min_interval in rate_limits config: %r", rate_limits["min_interval"]
                )

        # Mapping of (HTTP method, path) -> allowed requests per second
        self.rate_limits: dict[tuple[str, str], float] = {}
        for key, value in rate_limits.items():
            if isinstance(key, tuple) and len(key) == 2:
                method, path = key
            elif isinstance(key, str):
                try:
                    method, path = key.split(" ", 1)
                except ValueError:
                    LOG.warning("Invalid rate limit key: %r", key)
                    continue
            else:
                LOG.warning("Invalid rate limit key: %r", key)
                continue
            try:
                self.rate_limits[(method.upper(), path)] = float(value)
            except (TypeError, ValueError):
                LOG.warning("Invalid rate limit value for %s %s: %r", method, path, value)
        self.timeout = timeout
        # Track last-call timestamps for each (method, path) pair and globally
        self._last_call: dict[tuple[str, str], float] = {}
        self._last_request_ts = 0.0

    # --------------- Core HTTP ---------------

    def _sleep_for_rate(self, method: str, path: str) -> None:
        now = time.time()
        if self.min_interval > 0:
            delta = now - self._last_request_ts
            if delta < self.min_interval:
                delay = self.min_interval - delta
                LOG.debug("Delaying %s %s by %.2fs due to global rate limit", method, path, delay)
                time.sleep(delay)
                now += delay

        rate = self.rate_limits.get((method, path))
        if not rate or rate <= 0:
            return
        min_interval = 1.0 / rate
        last = self._last_call.get((method, path), 0.0)
        delta = now - last
        if delta < min_interval:
            delay = min_interval - delta
            LOG.debug("Delaying %s %s by %.2fs due to rate limit", method, path, delay)
            time.sleep(delay)

    def request(
        self, method: str, path: str, *, params: dict | None = None, json_body: Any | None = None
    ) -> requests.Response:
        url = f"{self.base_url}{path}"
        attempt = 0
        delay = self.retry.base_delay
        while True:
            attempt += 1
            self._sleep_for_rate(method, path)
            try:
                resp = self.session.request(
                    method, url, params=params, json=json_body, timeout=self.timeout
                )
            except requests.RequestException as e:
                if attempt >= self.retry.max_attempts:
                    LOG.error("HTTP error after %s attempts: %s", attempt, repr(e))
                    raise
                wait = min(self.retry.max_delay, delay * (2 ** (attempt - 1))) * (
                    1 + random.random() * 0.25
                )
                LOG.warning(
                    "HTTP exception on %s %s (attempt %s), retrying in %.2fs",
                    method,
                    path,
                    attempt,
                    wait,
                )
                time.sleep(wait)
                continue

            now = time.time()
            self._last_request_ts = now
            self._last_call[(method, path)] = now
            if resp.status_code in (429, 500, 502, 503, 504):
                if attempt >= self.retry.max_attempts:
                    LOG.error(
                        "HTTP %s after %s attempts: %s %s -> %s",
                        resp.status_code,
                        attempt,
                        method,
                        path,
                        resp.text[:400],
                    )
                    resp.raise_for_status()
                retry_after = resp.headers.get("Retry-After")
                if retry_after is not None:
                    try:
                        wait = float(retry_after)
                    except ValueError:
                        wait = min(self.retry.max_delay, delay * (2 ** (attempt - 1)))
                else:
                    wait = min(self.retry.max_delay, delay * (2 ** (attempt - 1)))
                wait *= 1 + random.random() * 0.25  # jitter
                LOG.warning(
                    "Rate/server error %s on %s %s (attempt %s), retrying in %.2fs",
                    resp.status_code,
                    method,
                    path,
                    attempt,
                    wait,
                )
                time.sleep(wait)
                continue

            # success or permanent error
            if resp.status_code >= 400:
                LOG.error(
                    "HTTP error %s on %s %s: %s", resp.status_code, method, path, resp.text[:400]
                )
            return resp

    # --------------- Endpoints ---------------

    def list_addresses(self, limit: int = 200) -> list[dict[str, Any]]:
        """Iterate through all addresses (handling pagination if present)."""
        out: list[dict[str, Any]] = []
        page_token: str | None = None
        token_param = "pageToken"
        while True:
            params = {"limit": limit}
            if page_token:
                params[token_param] = page_token
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

            pagination = data.get("pagination") or {}
            if pagination.get("hasNextPage"):
                page_token = pagination.get("endCursor")
                token_param = "after"
            else:
                page_token = data.get("nextPageToken") or pagination.get("nextPageToken")
                token_param = "pageToken"
            if not page_token:
                break
        return out

    def list_drivers(
        self, status: Literal["active", "deactivated"], limit: int = 200
    ) -> list[dict[str, Any]]:
        """Iterate through drivers filtered by status, handling pagination."""

        out: list[dict[str, Any]] = []
        page_token: str | None = None
        while True:
            params: dict[str, Any] = {"limit": limit, "status": status}
            if page_token:
                params["after"] = page_token
            r = self.request("GET", "/fleet/drivers", params=params)
            r.raise_for_status()
            data = r.json()
            items = data.get("data") or data.get("drivers") or data
            if isinstance(items, dict):
                items = items.get("drivers")
            if not isinstance(items, list):
                items = []
            out.extend(items)

            pagination = data.get("pagination") or {}
            page_token = (
                pagination.get("after")
                or pagination.get("nextPageToken")
                or data.get("nextPageToken")
            )
            if not page_token:
                break
        return out

    def list_all_drivers(self) -> list[dict[str, Any]]:
        """Return union of active and deactivated drivers."""

        combined: list[dict[str, Any]] = []
        seen_ids: set[Any] = set()
        for driver in self.list_drivers("active") + self.list_drivers("deactivated"):
            if not isinstance(driver, dict):
                combined.append(driver)
                continue
            driver_id = driver.get("id")
            if driver_id and driver_id in seen_ids:
                continue
            if driver_id:
                seen_ids.add(driver_id)
            combined.append(driver)
        return combined

    def get_driver(self, id_or_external: str) -> dict[str, Any] | None:
        r = self.request("GET", f"/fleet/drivers/{id_or_external}")
        if r.status_code == 404:
            return None
        r.raise_for_status()
        try:
            data = r.json()
        except ValueError:
            return None
        if isinstance(data, dict):
            nested = data.get("data") or data.get("driver")
            if isinstance(nested, dict):
                return nested
            return data
        return None

    def create_driver(self, payload: dict[str, Any]) -> dict[str, Any]:
        r = self.request("POST", "/fleet/drivers", json_body=payload)
        r.raise_for_status()
        return r.json()

    def patch_driver(self, id_or_external: str, payload: dict[str, Any]) -> dict[str, Any]:
        r = self.request("PATCH", f"/fleet/drivers/{id_or_external}", json_body=payload)
        r.raise_for_status()
        return r.json()

    def get_address(self, addr_id: str) -> dict[str, Any]:
        r = self.request("GET", f"/addresses/{addr_id}")
        r.raise_for_status()
        return r.json()

    def create_address(self, payload: dict[str, Any]) -> dict[str, Any]:
        r = self.request("POST", "/addresses", json_body=payload)
        try:
            r.raise_for_status()
        except requests.HTTPError as e:
            try:
                data = r.json()
            except ValueError:
                data = {}
            message = data.get("message")
            request_id = data.get("requestId")
            details = ", ".join(
                f"{k}: {v}" for k, v in (("message", message), ("requestId", request_id)) if v
            )
            LOG.error("Failed to create address payload=%s response=%s", payload, data)
            msg = str(e)
            if details:
                msg = f"{msg} ({details})"
            raise requests.HTTPError(msg, response=r) from e
        return r.json()

    def patch_address(self, addr_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        r = self.request("PATCH", f"/addresses/{addr_id}", json_body=payload)
        try:
            r.raise_for_status()
        except requests.HTTPError as e:
            try:
                data = r.json()
            except ValueError:
                data = {}
            message = data.get("message") or ""
            request_id = data.get("requestId")
            if (
                r.status_code == 400
                and "Duplicate external id value already exists" in message
            ):
                raise ExternalIdConflictError(message, response=r) from e
            if (
                r.status_code == 400
                and "Name must contain only letters or numbers" in message
            ):
                raise InvalidExternalIdKeyError(message, response=r) from e
            details = ", ".join(
                f"{k}: {v}" for k, v in (("message", message), ("requestId", request_id)) if v
            )
            LOG.error(
                "Failed to patch address id=%s payload=%s response=%s",
                addr_id,
                payload,
                data,
            )
            msg = str(e)
            if details:
                msg = f"{msg} ({details})"
            raise requests.HTTPError(msg, response=r) from e
        return r.json()

    def delete_address(self, addr_id: str) -> None:
        r = self.request("DELETE", f"/addresses/{addr_id}")
        r.raise_for_status()

    def list_tags(self, limit: int = 200) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
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
            page_token = data.get("nextPageToken") or data.get("pagination", {}).get(
                "nextPageToken"
            )
            if not page_token:
                break
        return out
