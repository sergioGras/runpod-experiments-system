"""Derive a conservative local time envelope from the single round cap."""
from decimal import Decimal, InvalidOperation, ROUND_FLOOR


def runtime_from_remaining(remaining_usd, hourly_usd, *, cleanup_seconds=120):
    """Return work seconds and total seconds based on a current hourly estimate.

    This is a planning estimate. Provider charges and price changes can exceed
    it; the estimate must include storage and other known ongoing charges.
    """
    if type(cleanup_seconds) is not int or cleanup_seconds < 0:
        raise ValueError("Invalid cleanup margin")
    try:
        remaining = Decimal(str(remaining_usd))
        hourly = Decimal(str(hourly_usd))
        if not remaining.is_finite() or not hourly.is_finite() or remaining <= 0 or hourly <= 0:
            raise ValueError("Remaining budget and hourly estimate must be positive")
        total_seconds = int((remaining * 3600 / hourly).to_integral_value(rounding=ROUND_FLOOR))
    except (InvalidOperation, ValueError, OverflowError) as error:
        raise ValueError("Invalid monetary estimate") from error
    if total_seconds <= cleanup_seconds:
        raise ValueError("Remaining budget cannot cover the cleanup margin")
    return {"work_seconds": total_seconds - cleanup_seconds, "total_seconds": total_seconds}
