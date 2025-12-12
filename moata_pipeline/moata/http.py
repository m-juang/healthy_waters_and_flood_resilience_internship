from __future__ import annotations
import time
import logging
from typing import Any, Dict, Optional
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)



class MoataHttp:
    def __init__(
        self,
        get_token_fn,
        base_url: str,
        requests_per_second: float = 2.0,
        verify_ssl: bool = False,
        timeout_seconds: int = 60,
    ) -> None:
        self._get_token = get_token_fn
        self._base_url = base_url.rstrip("/")
        self._sleep = 1.0 / max(requests_per_second, 0.1)
        self._verify_ssl = verify_ssl
        self._timeout = timeout_seconds

    def get(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        allow_404: bool = False,
        allow_403: bool = False,
    ) -> Any:
        url = f"{self._base_url}/{path.lstrip('/')}"
        time.sleep(self._sleep)

        headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Accept": "application/json",
        }

        resp = requests.get(url, headers=headers, params=params, timeout=self._timeout, verify=self._verify_ssl)

        if resp.status_code == 404 and allow_404:
            return None
        if resp.status_code == 403 and allow_403:
            return None

        # 401: refresh token implicitly via get_token() call
        if resp.status_code == 401:
            logging.warning("401 Unauthorized for %s - retrying once with refreshed token", url)
            headers["Authorization"] = f"Bearer {self._get_token()}"
            time.sleep(1)
            resp = requests.get(url, headers=headers, params=params, timeout=self._timeout, verify=self._verify_ssl)

            if resp.status_code == 404 and allow_404:
                return None
            if resp.status_code == 403 and allow_403:
                return None

        resp.raise_for_status()
        return resp.json()
