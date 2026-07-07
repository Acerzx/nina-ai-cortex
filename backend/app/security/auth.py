"""
Authentication & Authorization module for N.I.N.A. AI Cortex.

ИСПРАВЛЕНО (audit C4): добавлена JWT-аутентификация для защиты API.
Все критические endpoints (Execution, Vault, Simulation) требуют валидный токен.

Архитектура:
- JWT Bearer токены с HMAC-SHA256 подписью
- API key для machine-to-machine интеграций (Grafana, Home Assistant)
- Rate limiting через slowapi
- Разграничение прав: admin / operator / readonly
"""

import logging
import os
import secrets
from datetime import datetime, timedelta
from typing import Optional, List
from enum import Enum

from fastapi import Depends, HTTPException, status, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
from pydantic import BaseModel, Field

from app.core.config import settings

logger = logging.getLogger("SecurityAuth")


# ============================================================================
# MODELS
# ============================================================================


class UserRole(str, Enum):
    """Роли пользователей системы."""

    ADMIN = "admin"  # Полный доступ (настройка, vault, пользователи)
    OPERATOR = "operator"  # Управление сессиями, triggers, агентами
    READONLY = "readonly"  # Только чтение метрик и состояния


class TokenData(BaseModel):
    """Данные, извлечённые из JWT токена."""

    sub: str  # Идентификатор субъекта (user/client)
    role: UserRole = UserRole.READONLY
    exp: Optional[datetime] = None
    iat: Optional[datetime] = None
    scopes: List[str] = Field(default_factory=list)


class TokenResponse(BaseModel):
    """Ответ при выпуске нового токена."""

    access_token: str
    token_type: str = "bearer"
    expires_in: int  # секунд до истечения
    role: UserRole


class APIKeyRecord(BaseModel):
    """Запись об API-ключе (для machine-to-machine)."""

    name: str  # Человеко-читаемое имя
    key_hash: str  # Хеш ключа (не храним plaintext)
    role: UserRole
    created_at: str = Field(default_factory=lambda: datetime.now().isoformat())
    scopes: List[str] = Field(default_factory=list)


# ============================================================================
# CONFIGURATION
# ============================================================================

# Секрет для подписи JWT. В production ОБЯЗАТЕЛЬНО через env var.
_JWT_SECRET: Optional[str] = None


def _get_jwt_secret() -> str:
    """
    Возвращает JWT secret из env или генерирует эфемерный для dev.
    В production отсутствие JWT_SECRET — фатальная ошибка.
    """
    global _JWT_SECRET
    if _JWT_SECRET is not None:
        return _JWT_SECRET

    env_secret = os.getenv("JWT_SECRET")
    if env_secret:
        if len(env_secret) < 32:
            logger.warning(
                "⚠️ JWT_SECRET is shorter than 32 chars — "
                "consider using a stronger secret in production."
            )
        _JWT_SECRET = env_secret
        return _JWT_SECRET

    # Dev-режим: генерируем случайный эфемерный секрет
    if os.getenv("ENVIRONMENT", "development") == "production":
        raise RuntimeError(
            "JWT_SECRET environment variable MUST be set in production. "
            "Generate with: python -c 'import secrets; print(secrets.token_urlsafe(48))'"
        )

    _JWT_SECRET = secrets.token_urlsafe(48)
    logger.warning(
        "⚠️ JWT_SECRET not set — using ephemeral random secret. "
        "All tokens will be invalidated on restart."
    )
    return _JWT_SECRET


ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24  # 24 часа по умолчанию

# Пути, которые НЕ требуют аутентификации (public endpoints)
PUBLIC_PATHS = {
    "/",
    "/docs",
    "/openapi.json",
    "/redoc",
    "/health",
    "/api",
    "/api/v1",
    "/metrics",  # Prometheus scraping
}

# Префиксы путей, требующих конкретных ролей
ROLE_REQUIRED_PREFIXES = {
    "/api/v1/security": UserRole.ADMIN,
    "/api/v1/execution": UserRole.OPERATOR,
    "/api/v1/simulation": UserRole.OPERATOR,
    "/api/v1/agents/mode": UserRole.OPERATOR,
    "/api/v1/storage/cleanup": UserRole.OPERATOR,
}


# ============================================================================
# TOKEN OPERATIONS
# ============================================================================


def create_access_token(
    subject: str,
    role: UserRole = UserRole.READONLY,
    scopes: Optional[List[str]] = None,
    expires_delta: Optional[timedelta] = None,
) -> str:
    """
    Создаёт JWT access token.

    Args:
        subject: Идентификатор пользователя/клиента
        role: Роль (admin/operator/readonly)
        scopes: Дополнительные scopes (опционально)
        expires_delta: Кастомное время жизни

    Returns:
        Закодированный JWT токен
    """
    secret = _get_jwt_secret()
    now = datetime.utcnow()
    expire = now + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))

    payload = {
        "sub": subject,
        "role": role.value,
        "scopes": scopes or [],
        "iat": now,
        "exp": expire,
    }
    return jwt.encode(payload, secret, algorithm=ALGORITHM)


def decode_access_token(token: str) -> TokenData:
    """
    Декодирует и валидирует JWT токен.

    Raises:
        HTTPException 401 если токен невалиден или истёк.
    """
    secret = _get_jwt_secret()
    try:
        payload = jwt.decode(token, secret, algorithms=[ALGORITHM])
        return TokenData(
            sub=payload.get("sub", ""),
            role=UserRole(payload.get("role", "readonly")),
            scopes=payload.get("scopes", []),
        )
    except JWTError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid authentication token: {e}",
            headers={"WWW-Authenticate": "Bearer"},
        )


# ============================================================================
# API KEY SUPPORT (для machine-to-machine)
# ============================================================================

# In-memory хранилище API-ключей.
# В production рекомендуется вынести в Vault/SQLite.
_api_keys: dict[str, APIKeyRecord] = {}


def hash_api_key(key: str) -> str:
    """Хеширует API-ключ для безопасного хранения."""
    import hashlib

    return hashlib.sha256(key.encode()).hexdigest()


def register_api_key(
    name: str, role: UserRole, scopes: Optional[List[str]] = None
) -> str:
    """
    Регистрирует новый API-ключ.
    Возвращает plaintext ключ (показывается ОДИН раз при создании).
    """
    raw_key = f"cortex_{secrets.token_urlsafe(32)}"
    record = APIKeyRecord(
        name=name,
        key_hash=hash_api_key(raw_key),
        role=role,
        scopes=scopes or [],
    )
    _api_keys[record.key_hash] = record
    logger.info(f"🔑 API key registered: name={name}, role={role.value}")
    return raw_key


def validate_api_key(raw_key: str) -> Optional[TokenData]:
    """Валидирует API-ключ и возвращает TokenData или None."""
    if not raw_key.startswith("cortex_"):
        return None
    key_hash = hash_api_key(raw_key)
    record = _api_keys.get(key_hash)
    if not record:
        return None
    return TokenData(
        sub=f"apikey:{record.name}",
        role=record.role,
        scopes=record.scopes,
    )


# ============================================================================
# FASTAPI DEPENDENCIES
# ============================================================================

_bearer_scheme = HTTPBearer(auto_error=False)


async def get_current_user(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(_bearer_scheme),
) -> TokenData:
    """
    FastAPI-зависимость: извлекает и валидирует пользователя из запроса.

    Поддерживает два способа аутентификации:
    1. Bearer JWT токен (Authorization: Bearer <jwt>)
    2. API key (X-API-Key: cortex_...)

    Raises:
        HTTPException 401 если аутентификация не пройдена.
    """
    # 0. Пропускаем публичные пути
    path = request.url.path
    if path in PUBLIC_PATHS:
        return TokenData(sub="anonymous", role=UserRole.READONLY)

    # 0.1. Swagger UI и статика — пропускаем
    if path.startswith(("/docs", "/redoc", "/openapi.json")):
        return TokenData(sub="anonymous", role=UserRole.READONLY)

    # 0.2. WebSocket endpoint аутентифицируется отдельно
    if path == "/ws" or path == settings.ws_broadcast.path:
        return TokenData(sub="ws-client", role=UserRole.READONLY)

    # 0.3. Dev mode bypass (ТОЛЬКО если явно разрешено в конфиге)
    auth_config = getattr(settings, "auth", None)
    if auth_config and not auth_config.enabled:
        return TokenData(sub="dev-bypass", role=UserRole.ADMIN)

    # 1. Пробуем API key из заголовка X-API-Key
    api_key = request.headers.get("X-API-Key")
    if api_key:
        token_data = validate_api_key(api_key)
        if token_data:
            return token_data
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )

    # 2. Пробуем Bearer JWT токен
    if credentials and credentials.credentials:
        return decode_access_token(credentials.credentials)

    # 3. Аутентификация не предоставлена
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Missing authentication. Provide Bearer token or X-API-Key header.",
        headers={"WWW-Authenticate": "Bearer"},
    )


def require_role(required_role: UserRole):
    """
    FastAPI-зависимость: требует минимальную роль.
    Иерархия: ADMIN > OPERATOR > READONLY
    """
    role_hierarchy = {
        UserRole.ADMIN: 3,
        UserRole.OPERATOR: 2,
        UserRole.READONLY: 1,
    }

    async def _check(user: TokenData = Depends(get_current_user)) -> TokenData:
        if role_hierarchy.get(user.role, 0) < role_hierarchy[required_role]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    f"Insufficient permissions. "
                    f"Required: {required_role.value}, got: {user.role.value}"
                ),
            )
        return user

    return _check


# ============================================================================
# PATH-LEVEL AUTHORIZATION
# ============================================================================


def get_required_role_for_path(path: str) -> UserRole:
    """Определяет минимальную роль для доступа к пути."""
    for prefix, role in ROLE_REQUIRED_PREFIXES.items():
        if path.startswith(prefix):
            return role
    return UserRole.READONLY  # По умолчанию — чтение


async def authorize_request(
    request: Request,
    user: TokenData = Depends(get_current_user),
) -> TokenData:
    """
    Авторизует запрос на основе пути и роли пользователя.
    Автоматически вызывается для защищённых endpoints.
    """
    required = get_required_role_for_path(request.url.path)
    hierarchy = {UserRole.ADMIN: 3, UserRole.OPERATOR: 2, UserRole.READONLY: 1}

    if hierarchy.get(user.role, 0) < hierarchy[required]:
        logger.warning(
            f"🚫 Access denied: user={user.sub} role={user.role.value} "
            f"attempted {request.method} {request.url.path} "
            f"(requires {required.value})"
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Path requires {required.value} role",
        )
    return user


# ============================================================================
# UTILITY
# ============================================================================


def list_api_keys() -> List[dict]:
    """Возвращает список зарегистрированных API-ключей (без хешей)."""
    return [
        {
            "name": r.name,
            "role": r.role.value,
            "created_at": r.created_at,
            "scopes": r.scopes,
        }
        for r in _api_keys.values()
    ]


def revoke_api_key(name: str) -> bool:
    """Отзывает API-ключ по имени."""
    for key_hash, record in list(_api_keys.items()):
        if record.name == name:
            del _api_keys[key_hash]
            logger.info(f"🗑️ API key revoked: {name}")
            return True
    return False
