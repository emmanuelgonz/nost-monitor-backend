import os
import time

import httpx
import jwt
from fastapi import Depends, Header, HTTPException, status
from jwt import PyJWKClient

_KEYCLOAK_HOST = os.getenv("KEYCLOAK_HOST")
_KEYCLOAK_PORT = os.getenv("KEYCLOAK_PORT")
_KEYCLOAK_REALM = os.getenv("KEYCLOAK_REALM")
_USER_TOKEN_AUDIENCE = os.getenv("USER_TOKEN_AUDIENCE", "account")

_ISSUER = f"https://{_KEYCLOAK_HOST}:{_KEYCLOAK_PORT}/realms/{_KEYCLOAK_REALM}"
_JWKS_URL = f"{_ISSUER}/protocol/openid-connect/certs"
_JWKS_TTL_SECONDS = 15 * 60

_jwks_client: PyJWKClient | None = None
_jwks_fetched_at: float = 0.0


def _get_jwks_client() -> PyJWKClient:
    global _jwks_client, _jwks_fetched_at
    now = time.time()
    if _jwks_client is None or (now - _jwks_fetched_at) > _JWKS_TTL_SECONDS:
        _jwks_client = PyJWKClient(_JWKS_URL)
        _jwks_fetched_at = now
    return _jwks_client


def _verify_issuer_reachable() -> None:
    # Fail fast at import time if Keycloak is unreachable, with a clear message.
    try:
        httpx.get(f"{_ISSUER}/.well-known/openid-configuration", timeout=5.0, verify=False)
    except Exception as err:
        raise RuntimeError(f"Keycloak issuer unreachable at {_ISSUER}: {err}")


def require_auth(authorization: str | None = Header(default=None)) -> dict:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    token = authorization.split(" ", 1)[1].strip()

    try:
        signing_key = _get_jwks_client().get_signing_key_from_jwt(token).key
        claims = jwt.decode(
            token,
            signing_key,
            algorithms=["RS256"],
            audience=_USER_TOKEN_AUDIENCE,
            issuer=_ISSUER,
            options={"require": ["exp", "iss", "aud"]},
        )
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidAudienceError:
        raise HTTPException(status_code=401, detail="Invalid audience")
    except jwt.InvalidIssuerError:
        raise HTTPException(status_code=401, detail="Invalid issuer")
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

    return claims


def require_role(role_name: str):
    def dependency(claims: dict = Depends(require_auth)) -> dict:
        roles = claims.get("realm_access", {}).get("roles", [])
        if role_name not in roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Missing required role: {role_name}",
            )
        return claims

    return dependency
