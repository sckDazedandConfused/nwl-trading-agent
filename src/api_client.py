"""
api_client.py
HTTP helper for Schwab REST endpoints with pooled session, retries, and token refresh.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

import requests
from requests import Session, Response
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .token_manager import get_access_token, refresh_access_token

API_BASE = "https://api.schwabapi.com"

_session: Optional[Session] = None


def _session_pooled() -> Session:
    """Create/reuse a single Session with sane retries/backoff."""
    global _session
    if _session is None:
        s = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=0.4,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "POST", "PUT", "DELETE", "PATCH"),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        _session = s
    return _session


def _auth_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }


def _parse_json(resp: Response) -> Any:
    try:
        return resp.json()
    except ValueError:
        # Make debugging easier if the API returns HTML/text
        return {"status_code": resp.status_code, "text": resp.text}


def _request(method: str, endpoint: str, *, params=None, json_body=None, timeout: int = 30) -> Response:
    url = f"{API_BASE}/{endpoint.lstrip('/')}"
    s = _session_pooled()

    token = get_access_token()
    resp = s.request(method, url, headers=_auth_headers(token), params=params, json=json_body, timeout=timeout)

    if resp.status_code == 401:
        # Token likely expired—refresh once and retry
        refresh_access_token()
        token = get_access_token()
        resp = s.request(method, url, headers=_auth_headers(token), params=params, json=json_body, timeout=timeout)

    # Raise if still not ok (after retries handled by HTTPAdapter)
    resp.raise_for_status()
    return resp


# ---- Public helpers ---------------------------------------------------------

def get(endpoint: str, params: Optional[Dict[str, Any]] = None, timeout: int = 30) -> Any:
    resp = _request("GET", endpoint, params=params, timeout=timeout)
    return _parse_json(resp)


def post(endpoint: str, payload: Dict[str, Any], timeout: int = 30) -> Any:
    resp = _request("POST", endpoint, json_body=payload, timeout=timeout)
    return _parse_json(resp)
