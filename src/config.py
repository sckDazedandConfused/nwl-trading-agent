"""config.py
Centralised settings loader for Schwab API scripts.
Reads secrets from a `.env` file placed in the same folder.
Uses python-dotenv so nothing sensitive ever lives in source.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

env_path = Path(__file__).with_name('.env')
if env_path.exists():
    load_dotenv(env_path)
else:
    print('[config] Warning: .env file not found next to config.py')

class _Settings:
    CLIENT_ID     = os.getenv('SCHWAB_CLIENT_ID', '')
    CLIENT_SECRET = os.getenv('SCHWAB_CLIENT_SECRET', '')
    REDIRECT_URI  = os.getenv('SCHWAB_REDIRECT_URI', '')
    TOTP_SECRET   = os.getenv('SCHWAB_TOTP_SECRET', '')

    _required = ['CLIENT_ID', 'CLIENT_SECRET', 'REDIRECT_URI']
    for _name in _required:
        if not locals()[_name]:
            raise RuntimeError(f'[config] Missing required env variable: {_name}')

    def __repr__(self):
        return '<Settings (CLIENT_ID masked)>'

settings = _Settings()
