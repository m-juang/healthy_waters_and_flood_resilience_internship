from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional, Tuple

import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)


class MoataHttp:
    def __init__(
        self,
        get_token_fn,
        base_url: str,
        requests_per_second: float = 2.0,
        verify_ssl: bool = False,
        timeout_seconds: int = 60,
        # new knobs (safe defaults)
        connect_timeout_seconds: int = 15,
        max_retries: int = 5,
        backoff_factor: float = 1.0,
        pool_connections: int = 20,
        pool_maxsize: int = 20,
    ) -> None:
        self._get_token = get_token_fn
        self._base_url = base_url.rstrip("/")
        self._sleep = 1.0 / max(requests_per_second, 0.1)
        self._verify_ssl = verify_ssl

        # Use tuple timeout: (connect, read)
        self._timeout: Tuple[int, int] = (connect_timeout_seconds, timeout_seconds)

        self._session = requests.Session()

        retry = Retry(
            total=max_retries,
            connect=max_retries,
            read=max_retries,
            status=max_retries,
            backoff_factor=backoff_factor,  # 1s, 2s, 4s, 8s...
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
            respect_retry_after_header=True,
        )

        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=pool_connections,
            pool_maxsize=pool_maxsize,
        )

        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)

    def get(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        allow_404: bool = False,
        allow_403: bool = False,
    ) -> Any:
        url = f"{self._base_url}/{path.lstrip('/')}"

        # Simple rate limiting (client-side)
        time.sleep(self._sleep)

        headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Accept": "application/json",
        }

        try:
            resp = self._session.get(
                url,
                headers=headers,
                params=params,
                timeout=self._timeout,
                verify=self._verify_ssl,
            )
        except (requests.exceptions.SSLError, requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout) as e:
            # This catches the exact failure you saw (SSL handshake timeout / read timeout)
            logger.warning("Network/SSL timeout for %s params=%s: %s", url, params, e)
            raise

        if resp.status_code == 404 and allow_404:
            return None
        if resp.status_code == 403 and allow_403:
            return None

        # 401: refresh token and retry ONCE (not infinite)
        if resp.status_code == 401:
            logger.warning("401 Unauthorized for %s - retrying once with refreshed token", url)
            headers["Authorization"] = f"Bearer {self._get_token()}"
            time.sleep(1)

            resp = self._session.get(
                url,
                headers=headers,
                params=params,
                timeout=self._timeout,
                verify=self._verify_ssl,
            )

            if resp.status_code == 404 and allow_404:
                return None
            if resp.status_code == 403 and allow_403:
                return None

        # If server is overloaded/429, adapter retry has likely happened already.
        # Still fail loudly here if final status is not ok.
        resp.raise_for_status()

        # Some endpoints might return empty body; guard against JSON decode errors.
        if not resp.content:
            return None

        try:
            return resp.json()
        except ValueError:
            logger.warning("Non-JSON response for %s (status=%s): %s", url, resp.status_code, resp.text[:500])
            raise
