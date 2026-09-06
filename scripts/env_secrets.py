#!/usr/bin/env python3
"""
env_secrets.py — single source of truth for resolving pipeline secrets.

Database passwords live in the environment (a local .env file), never in
config.toml. Every reader in the pipeline — the extract, the loader, the sync
tool, and the GUI's config_manager — resolves passwords through resolve_secret()
here, so there is ONE mechanism and config.toml can be secret-free.

Environment variables:
    RAT_SOURCE_PWD        FileMaker source account password (may be blank)
    RAT_TARGET_PWD        Supabase / Postgres target password (legacy / 'supabase' profile)
    RAT_TARGET_PWD_<NAME> Password for target profile <NAME> (e.g. RAT_TARGET_PWD_OCI)
    RAT_TARGET_PROFILE    Which [database.target.<name>] profile is active
    RAT_OCI_SERVICE_KEY   Supabase service_role key for the oci instance's Storage API
                          (image upload scripts only -- never used for the anon-scoped
                          read path; resolve via plain resolve_secret(), not
                          resolve_target_pwd(), since only one instance has Storage today)
    RAT_OLD_CLOUD_ANON_KEY  anon key for the old Supabase.com cloud project being migrated
                          away from (migrate_storage_images_from_cloud.py's read side only
                          -- a one-off migration tool, not part of the ongoing pipeline)

Typical use:
    from env_secrets import resolve_secret, resolve_target_pwd, url_quote
    pwd = resolve_secret("RAT_TARGET_PWD", cfg_pwd)      # env wins, else config
    pwd = resolve_target_pwd("oci", cfg_pwd)             # profile-scoped password
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


def resolve_target_pwd(profile, cfg_val=None, cli_val=None, default=""):
    """Resolve the password for a named target-DB profile (e.g. "supabase", "oci").
    Precedence: CLI arg > env RAT_TARGET_PWD_<PROFILE> > (profile "supabase" only:
    legacy env RAT_TARGET_PWD) > config.toml value > default.

    The legacy bare RAT_TARGET_PWD is intentionally NOT a fallback for any profile
    other than "supabase" -- a new profile must get its own env var, so switching
    profiles can never silently authenticate against the wrong host with a stale
    password."""
    if cli_val is not None:
        return cli_val
    pwd = resolve_secret(f"RAT_TARGET_PWD_{profile.upper()}", cfg_val=None)
    if pwd:
        return pwd
    if profile == "supabase":
        pwd = resolve_secret("RAT_TARGET_PWD", cfg_val=None)
        if pwd:
            return pwd
    return cfg_val if cfg_val is not None else default


def url_quote(value):
    """Percent-encode a userinfo component (user or password) so special
    characters don't corrupt a postgresql:// / libpq connection URL."""
    return quote_plus(str(value or ""))
