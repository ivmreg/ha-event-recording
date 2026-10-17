"""Declarative validation foundation for Event Recording.

Strict boundary: Pure Python, zero Home Assistant imports, zero third-party/AI imports.
Designed for standalone use by Event Recording and future reuse by Event Vision.
Rejects boolean values and non-integral floats for integer specifications,
prohibits silent type coercion, and enforces strict boundary and character constraints.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from typing import Any


class ValidationError(ValueError):
    """Base error raised when validation fails against a FieldSpec or validator."""


@dataclass(frozen=True, slots=True)
class FieldSpec:
    """Base declarative specification for field validation."""

    name: str
    allow_none: bool = False
    default: Any = None

    def validate(self, val: Any) -> Any:
        """Validate input data against specification."""
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class NumberSpec(FieldSpec):
    """Declarative specification for numeric validation.

    Strictly rejects boolean values, non-number objects (no silent string coercion),
    non-finite values (NaN, Inf), huge-int overflows, and non-integral values when is_integer is True.
    """

    min_val: float | int | None = None
    max_val: float | int | None = None
    is_integer: bool = False
    integer: bool = False
    allow_zero: bool = True
    default: float | int | None = None

    def validate(self, val: Any) -> float | int | None:
        """Validate and return normalized numeric value or raise ValidationError."""
        return validate_number(
            val,
            name=self.name,
            min_val=self.min_val,
            max_val=self.max_val,
            is_integer=self.is_integer or self.integer,
            allow_none=self.allow_none,
            allow_zero=self.allow_zero,
            default=self.default,
        )


@dataclass(frozen=True, slots=True)
class StringSpec(FieldSpec):
    """Declarative specification for string validation.

    Enforces minimum and maximum length bounds, optional regex matching,
    and exclusion of ASCII control characters (0-31 and 127).
    """

    min_len: int = 1
    max_len: int = 255
    pattern: str | None = None
    disallow_control_chars: bool = True
    strip: bool = True
    default: str | None = None

    def validate(self, val: Any) -> str | None:
        """Validate and return normalized string or raise ValidationError."""
        if val is None:
            if self.allow_none:
                return self.default
            raise ValidationError(f"{self.name} cannot be None")

        if not isinstance(val, str):
            raise ValidationError(f"{self.name} must be a string, got {type(val).__name__}")

        clean = val.strip() if self.strip else val
        if not clean and self.min_len > 0:
            if self.allow_none:
                return self.default
            raise ValidationError(f"{self.name} cannot be empty")

        if len(clean) < self.min_len:
            raise ValidationError(f"{self.name} must be at least {self.min_len} characters")

        if len(clean) > self.max_len:
            raise ValidationError(f"{self.name} exceeds maximum length of {self.max_len}")

        if self.disallow_control_chars and any(ord(c) < 32 or ord(c) == 127 for c in clean):
            raise ValidationError(f"{self.name} contains prohibited control characters")

        if self.pattern and not re.match(self.pattern, clean):
            raise ValidationError(f"Invalid characters in {self.name}")

        return clean


@dataclass(frozen=True, slots=True)
class IdSpec(StringSpec):
    """Declarative specification for safe entity, event, and media identifiers."""

    min_len: int = 1
    max_len: int = 64
    pattern: str | None = r"^[a-zA-Z0-9_-]+$"
    disallow_control_chars: bool = True
    strip: bool = True
    default: str | None = None

    def validate(self, val: Any) -> str:
        """Validate identifier rejecting control characters and traversal sequences."""
        if isinstance(val, str) and any(seq in val for seq in ("/", "\\", "..")):
            raise ValidationError(f"Invalid characters or path traversal attempt in {self.name}")
        res = StringSpec.validate(self, val)
        assert res is not None
        return res


def validate_number(
    val: Any,
    name: str = "number",
    *,
    min_val: float | int | None = None,
    max_val: float | int | None = None,
    is_integer: bool = False,
    integer: bool = False,
    allow_none: bool = False,
    allow_zero: bool = True,
    default: float | int | None = None,
) -> float | int | None:
    """Validate numeric value enforcing type, finiteness, integrality, and bounds.

    Strict rules:
    - Booleans are rejected (bool is an int subclass in Python).
    - Numeric strings are rejected (no silent type coercion).
    - Finite float conversion catches OverflowError for huge integers.
    - NaN and +/- Inf are rejected.
    - If is_integer (or integer) is True, float inputs must be exact integers (e.g. 5.0 -> 5).
    - Diagnostic error messages never leak the raw input value.
    """
    if val is None:
        if allow_none:
            return default
        raise ValidationError(f"{name} cannot be None")

    if isinstance(val, bool):
        raise ValidationError(f"{name} must not be a boolean (must be a number, got bool)")
    if not isinstance(val, (int, float)):
        raise ValidationError(f"{name} must be a number, got {type(val).__name__}")

    # Check finite float and catch huge integer OverflowError
    try:
        num_float = float(val)
    except OverflowError as err:
        raise ValidationError(f"{name} value is too large to represent as a finite number") from err

    if not math.isfinite(num_float):
        raise ValidationError(f"{name} must be finite")

    require_int = is_integer or integer
    if require_int:
        if isinstance(val, float) and not val.is_integer():
            raise ValidationError(f"{name} must be an integer")
        result: float | int = int(val)
    else:
        result = float(val) if isinstance(val, int) else val

    # Zero allowance
    if not allow_zero and result == 0:
        raise ValidationError(f"{name} cannot be zero")

    # Lower bound
    if min_val is not None:
        if allow_zero and result == 0 and min_val <= 0:
            pass
        elif not allow_zero and result <= min_val:
            raise ValidationError(f"{name} must be > {min_val}")
        elif result < min_val:
            raise ValidationError(f"{name} must be >= {min_val}")

    # Upper bound
    if max_val is not None and result > max_val:
        raise ValidationError(f"{name} must be <= {max_val}")

    return result


def validate_finite_number(
    val: Any,
    name: str,
    min_val: float = 0.0,
    allow_zero: bool = True,
) -> float:
    """Validate a finite, non-NaN positive float or int. Preserves legacy models.py signature."""
    res = validate_number(
        val,
        name=name,
        min_val=min_val,
        allow_zero=allow_zero,
        is_integer=False,
    )
    assert res is not None
    return float(res)


def validate_bounded_string(
    val: Any,
    name: str,
    max_len: int = 255,
    allow_none: bool = False,
    min_len: int = 1,
) -> str | None:
    """Validate a bounded non-sensitive string without control characters."""
    spec = StringSpec(
        name=name,
        min_len=min_len,
        max_len=max_len,
        allow_none=allow_none,
        disallow_control_chars=True,
        strip=True,
    )
    return spec.validate(val)


def validate_safe_id(val: Any, name: str = "id", max_len: int = 64) -> str:
    """Validate safe internal/external identifier rejecting path components and control chars."""
    spec = IdSpec(name=name, max_len=max_len)
    return spec.validate(val)


def validate_correlation_id(val: Any, name: str = "correlation_id") -> str | None:
    """Validate optional correlation ID string (up to 64 chars, safe characters)."""
    if val is None:
        return None
    spec = StringSpec(
        name=name,
        min_len=1,
        max_len=64,
        pattern=r"^[a-zA-Z0-9_\-.:]+$",
        allow_none=True,
        disallow_control_chars=True,
        strip=True,
    )
    return spec.validate(val)


def validate_payload_json(
    payload: Any,
    name: str = "payload",
    max_bytes: int = 131072,
) -> dict[str, Any]:
    """Validate JSON payload dictionary and reject NaN, inf, or oversized blobs."""
    if payload is None:
        return {}
    if not isinstance(payload, dict):
        raise ValidationError(f"{name} must be a dictionary, got {type(payload).__name__}")
    try:
        dumped = json.dumps(payload, allow_nan=False)
    except (ValueError, TypeError) as err:
        raise ValidationError(f"{name} cannot be serialized to valid finite JSON: {err}") from err
    if len(dumped.encode("utf-8")) > max_bytes:
        raise ValidationError(f"{name} exceeds maximum JSON size of {max_bytes} bytes")
    return payload


# Canonical declarative specifications for Event Recording
SPEC_RETENTION_SIZE_MB = NumberSpec(
    name="retention_size_mb", min_val=50, is_integer=True, default=1000
)
SPEC_RETENTION_DAYS = NumberSpec(
    name="retention_days", min_val=1, is_integer=True, default=7
)
SPEC_MIN_DISK_FREE_MB = NumberSpec(
    name="min_disk_free_mb", min_val=50, max_val=10240, is_integer=True, default=500
)
SPEC_CHUNK_SECONDS = NumberSpec(
    name="chunk_seconds", min_val=1, max_val=15, is_integer=True, default=10
)
SPEC_MAX_PARALLEL_RESOURCES = NumberSpec(
    name="max_parallel_resources", min_val=1, max_val=8, is_integer=True, default=2
)
SPEC_MAX_QUEUE_DEPTH = NumberSpec(
    name="max_queue_depth", min_val=1, max_val=256, is_integer=True, default=32
)
SPEC_QUEUE_WAIT_TIMEOUT_SECONDS = NumberSpec(
    name="queue_wait_timeout_seconds", min_val=1, max_val=300, is_integer=True, default=30
)
SPEC_AUTO_ASSOCIATION_TOLERANCE_SECONDS = NumberSpec(
    name="auto_association_tolerance_seconds",
    min_val=1.0,
    max_val=60.0,
    is_integer=False,
    default=15.0,
)
SPEC_MAX_INCIDENT_SPAN_SECONDS = NumberSpec(
    name="max_incident_span_seconds",
    min_val=10.0,
    max_val=3600.0,
    is_integer=False,
    default=300.0,
)
SPEC_WINDOW_SECONDS = NumberSpec(
    name="window_seconds", min_val=1, max_val=300, is_integer=True, default=15
)
SPEC_LOOKBACK_SECONDS = NumberSpec(
    name="lookback_seconds", min_val=0, max_val=60, is_integer=True, default=0
)
SPEC_MAX_WINDOW_SECONDS = NumberSpec(
    name="max_window_seconds", min_val=1, max_val=300, is_integer=True, default=300
)
SPEC_EXPORT_DELAY_SECONDS = NumberSpec(
    name="export_delay_seconds", min_val=0, max_val=60, is_integer=True, default=5
)
SPEC_TAIL_WINDOW_SECONDS = NumberSpec(
    name="tail_window_seconds", min_val=0.0, max_val=300.0, is_integer=False, default=0.0
)
SPEC_SOURCE_CLOCK_OFFSET_SECONDS = NumberSpec(
    name="source_clock_offset_seconds",
    min_val=-3600.0,
    max_val=3600.0,
    is_integer=False,
    default=0.0,
)
SPEC_SOURCE_TIME_UNCERTAINTY_SECONDS = NumberSpec(
    name="source_time_uncertainty_seconds",
    min_val=0.0,
    max_val=300.0,
    is_integer=False,
    default=0.0,
)
SPEC_SOURCE_ID = StringSpec(name="source_id", min_len=1, max_len=128)
SPEC_LABEL = StringSpec(name="label", min_len=1, max_len=256)
SPEC_GROUP_OR_ZONE = StringSpec(
    name="group_or_zone", min_len=1, max_len=64, disallow_control_chars=True, strip=False
)
SPEC_FRIGATE_CAMERA_NAME = IdSpec(name="frigate_camera_name", max_len=128)

