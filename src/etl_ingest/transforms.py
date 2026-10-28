"""Normalisers that turn messy source values into one consistent format.

Each normaliser takes a raw value (usually a string from a CSV cell) and
returns the cleaned value, ``None`` for "no value", or raises ``ValueError``
with a short, human-readable reason. The pipeline turns those reasons into
entries in the data-quality report.
"""

from __future__ import annotations

import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Optional

EMPTY_MARKERS = {"", "n/a", "na", "null", "none", "-", "--"}

DEFAULT_DATE_FORMATS = [
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%d/%m/%Y",
    "%d-%m-%Y",
    "%d %b %Y",
    "%d-%b-%Y",
    "%b %d, %Y",
    "%B %d, %Y",
    "%Y%m%d",
]

EMAIL_RE = re.compile(r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$")

# Small built-in lookup. Extend it per client via the pipeline config
# ("country_aliases"), rather than editing code.
COUNTRIES = {
    "KE": ["kenya", "ken"],
    "US": ["united states", "united states of america", "usa", "u.s.", "u.s.a.", "america"],
    "GB": ["united kingdom", "uk", "u.k.", "great britain", "england", "gbr"],
    "UG": ["uganda", "uga"],
    "TZ": ["tanzania", "tza"],
    "NG": ["nigeria", "nga"],
    "ZA": ["south africa", "zaf"],
    "CA": ["canada", "can"],
    "DE": ["germany", "deu", "deutschland"],
    "IN": ["india", "ind"],
}
_COUNTRY_LOOKUP = {code.lower(): code for code in COUNTRIES}
for _code, _names in COUNTRIES.items():
    for _name in _names:
        _COUNTRY_LOOKUP[_name] = _code


def normalize_header(name: str) -> str:
    """'  Signup Date ' -> 'signup_date', 'E-mail' -> 'e_mail'."""
    cleaned = re.sub(r"[^a-z0-9]+", "_", name.strip().lower())
    return cleaned.strip("_")


def is_empty(value: Any) -> bool:
    return value is None or (isinstance(value, str) and value.strip().lower() in EMPTY_MARKERS)


def to_string(value: Any, **_: Any) -> Optional[str]:
    if is_empty(value):
        return None
    return re.sub(r"\s+", " ", str(value)).strip()


def to_email(value: Any, **_: Any) -> Optional[str]:
    text = to_string(value)
    if text is None:
        return None
    email = text.lower()
    if not EMAIL_RE.match(email):
        raise ValueError("not a valid email address")
    return email


def to_date(value: Any, date_formats: Optional[list[str]] = None, **_: Any) -> Optional[str]:
    text = to_string(value)
    if text is None:
        return None
    for fmt in date_formats or DEFAULT_DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    raise ValueError("unrecognised date format")


def to_phone(value: Any, **_: Any) -> Optional[str]:
    """Keep digits, preserve an international prefix ('+' or '00' -> '+')."""
    text = to_string(value)
    if text is None:
        return None
    international = text.startswith("+") or text.startswith("00")
    digits = re.sub(r"\D", "", text)
    if text.startswith("00"):
        digits = digits[2:]
    if not 7 <= len(digits) <= 15:
        raise ValueError("phone number must have 7-15 digits")
    return f"+{digits}" if international else digits


def to_country(value: Any, country_aliases: Optional[dict[str, str]] = None, **_: Any) -> Optional[str]:
    text = to_string(value)
    if text is None:
        return None
    key = text.lower()
    if country_aliases:
        extra = {k.lower(): v.upper() for k, v in country_aliases.items()}
        if key in extra:
            return extra[key]
    if key in _COUNTRY_LOOKUP:
        return _COUNTRY_LOOKUP[key]
    raise ValueError("unknown country")


def to_decimal(value: Any, **_: Any) -> Optional[str]:
    """'$1,250.50' -> '1250.50'. Returned as a string so no precision is lost."""
    text = to_string(value)
    if text is None:
        return None
    cleaned = re.sub(r"[^\d.\-]", "", text)
    try:
        number = Decimal(cleaned)
    except InvalidOperation:
        raise ValueError("not a number") from None
    if not number.is_finite():
        raise ValueError("not a number")
    return str(number)


def to_int(value: Any, **_: Any) -> Optional[int]:
    text = to_string(value)
    if text is None:
        return None
    cleaned = text.replace(",", "")
    if not re.fullmatch(r"-?\d+", cleaned):
        raise ValueError("not a whole number")
    return int(cleaned)


def to_bool(value: Any, **_: Any) -> Optional[bool]:
    text = to_string(value)
    if text is None:
        return None
    key = text.lower()
    if key in {"true", "yes", "y", "1", "t"}:
        return True
    if key in {"false", "no", "n", "0", "f"}:
        return False
    raise ValueError("not a yes/no value")


NORMALIZERS: dict[str, Callable[..., Any]] = {
    "string": to_string,
    "email": to_email,
    "date": to_date,
    "phone": to_phone,
    "country": to_country,
    "decimal": to_decimal,
    "int": to_int,
    "bool": to_bool,
}
