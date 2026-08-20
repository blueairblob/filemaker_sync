#!/usr/bin/env python3
"""
env_secrets.py — single source of truth for resolving pipeline secrets.

Database passwords live in the environment (a local .env file), never in
config.toml. Every reader in the pipeline — the extract, the loader, the sync
tool, and the GUI's config_manager — resolves passwords through resolve_secret()
here, so there is ONE mechanism and config.toml can be secret-free.

Environment variables:
    RAT_SOURCE_PWD   FileMaker source account password (may be blank)
    RAT_TARGET_PWD   Supabase / Postgres target database password

Typical use:
    from env_secrets import resolve_secret, url_quote
    pwd = resolve_secret("RAT_TARGET_PWD", cfg_pwd)      # env wins, else config
    url = f"postgresql://{user}:{url_quote(pwd)}@{host}:{port}/{db}"

.env is loaded automatically (if python-dotenv is installed) the first time a
secret is resolved; manually-exported environment variables work regardless.
"""
from __future__ import annotations
import os
from urllib.parse import quote_plus

_ENV_LOADED = False


def load_env(dotenv_path=None) -> None:
    """Load a local .env into the process environment, once. No-op if
    python-dotenv isn't installed (exported env vars still apply)."""
    global _ENV_LOADED
    if _ENV_LOADED:
        return
    try:
        from dotenv import load_dotenv
        load_dotenv(dotenv_path)
    except ModuleNotFoundError:
        pass
    _ENV_LOADED = True


def resolve_secret(env_key, cfg_val=None, cli_val=None, default=""):
    """Resolve a secret. Precedence: explicit CLI arg > environment variable
    (incl. .env) > config.toml value > default."""
    load_env()
    if cli_val is not None:
        return cli_val
    env_val = os.environ.get(env_key)
    if env_val:                       # a non-empty env var wins over config
        return env_val
    return cfg_val if cfg_val is not None else default


def url_quote(value):
    """Percent-encode a userinfo component (user or password) so special
    characters don't corrupt a postgresql:// / libpq connection URL."""
    return quote_plus(str(value or ""))
