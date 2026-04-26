"""
BaoStock API rate limiter and compliance module

Ensures all BaoStock API calls comply with:
- Daily request limit (50,000 per IP)
- Inter-request delay (10-50ms recommended)
- Blacklist detection (error code 10001011)
- Date range validation (data available from 2015-01-01)
"""

import json
import logging
import os
import tempfile
import threading
import time
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class BaoStockBlacklistError(Exception):
    """IP blacklisted by BaoStock. Stop all requests immediately."""


class BaoStockDailyLimitExceeded(Exception):
    """Daily request limit reached (95% threshold)."""


class BaoStockLoginLimitError(Exception):
    """Concurrent login limit reached (error 10001005)."""


@dataclass
class BaoStockRateLimitConfig:
    daily_limit: int = 50_000
    warn_threshold: float = 0.80
    stop_threshold: float = 0.95
    inter_request_delay: float = 0.05  # 50ms
    blacklist_error_code: str = "10001011"
    login_limit_error_code: str = "10001005"
    earliest_date: str = "2015-01-01"
    counter_file: str = ".baostock_daily_counter.json"


# Resolve data directory for counter file
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_DATA_DIR = _PROJECT_ROOT / "data"


def _default_counter_path() -> Path:
    return _DATA_DIR / BaoStockRateLimitConfig().counter_file


class BaoStockRateLimiter:
    """Thread-safe rate limiter with persisted daily request counting."""

    def __init__(self, config: Optional[BaoStockRateLimitConfig] = None):
        self._config = config or BaoStockRateLimitConfig()
        self._lock = threading.Lock()
        self._counter_path = Path(self._config.counter_file)
        if not self._counter_path.is_absolute():
            self._counter_path = _DATA_DIR / self._config.counter_file
        self._date: str = ""
        self._count: int = 0
        self._load_counter()

    # ── Counter persistence ──────────────────────────────────────

    def _load_counter(self) -> None:
        today = date.today().isoformat()
        if self._counter_path.exists():
            try:
                with open(self._counter_path, "r") as f:
                    data = json.load(f)
                if data.get("date") == today:
                    self._date = data["date"]
                    self._count = data.get("count", 0)
                    return
            except (json.JSONDecodeError, OSError, KeyError):
                pass
        self._date = today
        self._count = 0

    def _save_counter(self) -> None:
        self._counter_path.parent.mkdir(parents=True, exist_ok=True)
        data = {"date": self._date, "count": self._count}
        try:
            fd, tmp = tempfile.mkstemp(
                dir=str(self._counter_path.parent), suffix=".tmp"
            )
            with os.fdopen(fd, "w") as f:
                json.dump(data, f)
            os.replace(tmp, str(self._counter_path))
        except OSError:
            try:
                with open(self._counter_path, "w") as f:
                    json.dump(data, f)
            except OSError:
                pass

    # ── Core gate ────────────────────────────────────────────────

    def check_and_increment(self) -> None:
        """Call before every BaoStock API request.

        Applies inter-request delay, checks daily limit, increments counter.
        """
        with self._lock:
            today = date.today().isoformat()
            if self._date != today:
                self._date = today
                self._count = 0

            stop_at = int(self._config.daily_limit * self._config.stop_threshold)
            if self._count >= stop_at:
                raise BaoStockDailyLimitExceeded(
                    "BaoStock daily limit reached: %d/%d requests used today (%s). "
                    "Stop threshold: %d%%. Resume tomorrow."
                    % (
                        self._count,
                        self._config.daily_limit,
                        self._date,
                        int(self._config.stop_threshold * 100),
                    )
                )

            warn_at = int(self._config.daily_limit * self._config.warn_threshold)
            if self._count >= warn_at:
                logger.warning(
                    "BaoStock daily quota warning: %d/%d (%.0f%%) used",
                    self._count,
                    self._config.daily_limit,
                    self._count / self._config.daily_limit * 100,
                )

            self._count += 1
            self._save_counter()

        time.sleep(self._config.inter_request_delay)

    # ── Blacklist detection ──────────────────────────────────────

    def check_blacklist(self, error_code: str, error_msg: str = "") -> None:
        """Raise BaoStockBlacklistError if error_code is 10001011."""
        if error_code == self._config.blacklist_error_code:
            raise BaoStockBlacklistError(
                "IP has been BLACKLISTED by BaoStock (error %s). "
                "All requests will be rejected. "
                "Contact QQ group admin with your public IP to get unblocked. "
                "See: https://www.baostock.com/blacklist"
                % self._config.blacklist_error_code
            )

    def check_login_error(self, error_code: str, error_msg: str = "") -> None:
        """Check login-specific errors with granular exceptions."""
        if error_code == self._config.blacklist_error_code:
            raise BaoStockBlacklistError(
                "IP blacklisted during BaoStock login. "
                "Contact QQ group admin with your public IP. "
                "See: https://www.baostock.com/blacklist"
            )
        if error_code == self._config.login_limit_error_code:
            raise BaoStockLoginLimitError(
                "BaoStock concurrent login limit reached (error %s). "
                "Retry after a few minutes." % error_code
            )

    # ── Date validation ──────────────────────────────────────────

    def validate_date(self, start_date: str) -> str:
        """Clamp start_date to earliest supported date if needed."""
        if start_date < self._config.earliest_date:
            logger.debug(
                "Clamping start_date from %s to %s (BaoStock earliest)",
                start_date,
                self._config.earliest_date,
            )
            return self._config.earliest_date
        return start_date

    # ── Status reporting ─────────────────────────────────────────

    def get_status(self) -> dict:
        with self._lock:
            today = date.today().isoformat()
            count = self._count if self._date == today else 0
        return {
            "date": today,
            "count": count,
            "limit": self._config.daily_limit,
            "remaining": self._config.daily_limit - count,
            "percentage": count / self._config.daily_limit * 100,
        }


# ── Module-level singleton ───────────────────────────────────────

_limiter: Optional[BaoStockRateLimiter] = None
_limiter_lock = threading.Lock()


def get_baostock_rate_limiter(
    config: Optional[BaoStockRateLimitConfig] = None,
) -> BaoStockRateLimiter:
    global _limiter
    if _limiter is None:
        with _limiter_lock:
            if _limiter is None:
                _limiter = BaoStockRateLimiter(config)
    return _limiter


def check_baostock_blacklist(error_code: str, error_msg: str = "") -> None:
    """Standalone blacklist check (no limiter instance needed)."""
    if error_code == "10001011":
        raise BaoStockBlacklistError(
            "IP has been BLACKLISTED by BaoStock. "
            "Contact QQ group admin with your public IP. "
            "See: https://www.baostock.com/blacklist"
        )
