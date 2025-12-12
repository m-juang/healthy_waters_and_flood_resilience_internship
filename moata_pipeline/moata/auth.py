from __future__ import annotations
import time
import logging
from dataclasses import dataclass
from typing import Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

@dataclass
class Token:
    access_token: str
    acquired_at: float
    ttl_seconds: int = 3600

    def expires_in(self) -> float:
        return self.ttl_seconds - (time.time() - self.acquired_at)

    def near_expiry(self, buffer_seconds: int = 300) -> bool:
        return self.expires_in() <= buffer_seconds


class MoataAuth:
    def __init__(
        self,
        token_url: str,
        scope: str,
        client_id: str,
        client_secret: str,
        verify_ssl: bool = False,
        timeout_seconds: int = 30,
        ttl_seconds: int = 3600,
        refresh_buffer_seconds: int = 300,
    ) -> None:
        self._token_url = token_url
        self._scope = scope
        self._client_id = client_id
        self._client_secret = client_secret
        self._verify_ssl = verify_ssl
        self._timeout = timeout_seconds
        self._ttl = ttl_seconds
        self._buffer = refresh_buffer_seconds
        self._token: Optional[Token] = None

    def get_token(self) -> str:
        if self._token is None or self._token.near_expiry(self._buffer):
            self._token = self._request_token()
        return self._token.access_token

    def _request_token(self) -> Token:
        logging.info("Requesting access token...")

        data = {
            "client_id": self._client_id,
            "client_secret": self._client_secret,
            "grant_type": "client_credentials",
        }
        params = {"scope": self._scope}

        # Robust session + retries for flaky TLS/connection resets
        session = requests.Session()
        retries = Retry(
            total=5,
            connect=5,
            read=5,
            backoff_factor=1.0,             # 1s, 2s, 4s, 8s...
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods={"POST"},
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        try:
            resp = session.post(
                self._token_url,
                data=data,
                params=params,
                timeout=self._timeout,
                verify=self._verify_ssl,
            )
        except requests.exceptions.ConnectionError as e:
            # Last resort small sleep + one final direct try
            logging.warning("Token request connection error: %s", e)
            time.sleep(2)
            resp = session.post(
                self._token_url,
                data=data,
                params=params,
                timeout=self._timeout,
                verify=self._verify_ssl,
            )

        resp.raise_for_status()
        payload = resp.json()

        token = payload.get("access_token")
        if not token:
            raise RuntimeError(f"No access_token in response: {payload}")

        logging.info("Successfully obtained access token.")
        return Token(access_token=token, acquired_at=time.time(), ttl_seconds=self._ttl)
