"""@cached decorator and TTL configuration for data types."""

import hashlib
from functools import wraps

from simtradedata.cache.cache import MemoryCache

# TTL configuration per data type (seconds)
DEFAULT_TTL = {
    "stock_list": 86400,       # 24h
    "daily_kline": 604800,     # 7 days
    "snapshot": 5,             # 5s
    "fundamentals": 3600,      # 1h
    "trade_calendar": 604800,  # 7 days
}

_default_cache = MemoryCache(max_size=1000)


def get_default_cache() -> MemoryCache:
    """Return the module-level shared cache instance."""
    return _default_cache


def _make_key(prefix: str, args: tuple, kwargs: dict) -> str:
    """Build a cache key from prefix, args, and kwargs."""
    raw = f"{prefix}:{args!r}:{sorted(kwargs.items())!r}"
    return hashlib.md5(raw.encode()).hexdigest()


def cached(ttl: float, key_prefix: str):
    """
    Decorator that caches function return values.

    Args:
        ttl: Time-to-live in seconds.
        key_prefix: Prefix for cache key (should be unique per function).

    The decorated function gains two extra methods:
    - ``func.invalidate(*args, **kwargs)`` -- remove cached entry
    - ``func.nocache(*args, **kwargs)`` -- call without cache
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            key = _make_key(key_prefix, args, kwargs)
            result = _default_cache.get(key)
            if result is not None:
                return result
            result = func(*args, **kwargs)
            _default_cache.set(key, result, ttl)
            return result

        def invalidate(*args, **kwargs):
            key = _make_key(key_prefix, args, kwargs)
            _default_cache.delete(key)

        def nocache(*args, **kwargs):
            return func(*args, **kwargs)

        wrapper.invalidate = invalidate
        wrapper.nocache = nocache
        return wrapper

    return decorator
