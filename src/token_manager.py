"""
token_manager.py
Minimal, test-aware token manager.

- In TEST mode (PYTHON_ENV=test): returns a fixed dummy token; refresh is a no-op.
- In non-test mode: reads/writes a simple JSON token file on disk.
  This is a placeholder for a real OAuth flow and is safe to import.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from .config import settings  # ensures env vars are loaded (dummy in tests)

# ---- Configuration ----------------------------------------------------------

# Allow overriding token path via env; default to repo root token filename
DEFAULT_TOKEN_FILENAME = os.getenv("SCHWAB_TOKEN_FILENAME", "marketdata_token.json")

# Resolve token file at repo root (../ from src/)
REPO_ROOT = Path(__file__).resolve().parents[1]
TOKEN_PATH = (REPO_ROOT / DEFAULT_TOKEN_FILENAME).resolve()

TEST_MODE = os.getenv("PYTHON_ENV") == "test"


@dataclass
class TokenRecord:
    access_token: str
    expires_at: Optional[str] = None  # ISO UTC string e.g. "2025-08-11T15:04:05Z"

    @property
    def is_expired(self) -> bool:
        if not self.expires_at:
            return False
        try:
            dt = datetime.fromisoformat(self.expires_at.replace("Z", "+00:00"))
            return datetime.now(timezone.utc) >= dt
        except Exception:
            # If parse fails, assume not expired to avoid breaking imports
            return False


# ---- Disk helpers -----------------------------------------------------------

def _load_token_from_disk() -> Optional[TokenRecord]:
    if not TOKEN_PATH.exists():
        return None
    try:
        data = json.loads(TOKEN_PATH.read_text(encoding="utf-8"))
        return TokenRecord(
            access_token=data.get("access_token", ""),
            expires_at=data.get("expires_at"),
        )
    except Exception:
        return None


def _save_token_to_disk(token: TokenRecord) -> None:
    payload = {
        "access_token": token.access_token,
        "expires_at": token.expires_at,
    }
    TOKEN_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")


# ---- Public API -------------------------------------------------------------

def get_access_token() -> str:
    """
    Return a bearer token string suitable for Authorization: Bearer <token>.

    TEST_MODE: always return 'test-token' (no I/O, no network).
    Non-test: read token file; if missing, create a placeholder and return it.
    """
    if TEST_MODE:
        return "test-token"

    tok = _load_token_from_disk()
    if tok is None:
        # Create a placeholder token so the rest of the app can run locally.
        # Expires in 30 minutes to simulate rotation, but you can adjust.
        placeholder = TokenRecord(
            access_token="local-placeholder-token",
            expires_at=(datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat().replace("+00:00", "Z"),
        )
        _save_token_to_disk(placeholder)
        return placeholder.access_token

    # If expired, a caller may choose to call refresh_access_token() first.
    return tok.access_token


def refresh_access_token() -> None:
    """
    Refresh the token. In TEST_MODE this is a no-op.

    In non-test mode, we simulate a refresh by replacing the file with a new token
    and a future expiry. Replace this with your real OAuth flow later.
    """
    if TEST_MODE:
        return

    new_token = TokenRecord(
        access_token="local-refreshed-token",
        expires_at=(datetime.now(timezone.utc) + timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
    )
    _save_token_to_disk(new_token)
