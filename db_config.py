"""Shared database configuration — loads credentials from .env"""
import os

_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
if os.path.exists(_env_path):
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith('#') and '=' in _line:
                _key, _, _val = _line.partition('=')
                os.environ.setdefault(_key.strip(), _val.strip())

TOKEN = os.environ.get('SUPABASE_MANAGEMENT_TOKEN', '')
PROJECT_REF = os.environ.get('SUPABASE_PROJECT_REF', 'pikcvwulzfxgwfcfssxc')
API_URL = f'https://api.supabase.com/v1/projects/{PROJECT_REF}/database/query'

if not TOKEN:
    raise RuntimeError('SUPABASE_MANAGEMENT_TOKEN not found — check .env file.')
