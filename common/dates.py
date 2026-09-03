from __future__ import annotations

from datetime import date, datetime, timedelta


def parse_date_arg(raw: str) -> date:
    return datetime.strptime(raw, "%Y-%m-%d").date()


def date_to_ts_ms(value: date) -> int:
    return int(datetime.combine(value, datetime.min.time()).timestamp() * 1000)


def coerce_to_date(value) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    try:
        import pandas as pd

        if isinstance(value, pd.Timestamp):
            return value.date()
        if isinstance(value, float) and pd.isna(value):
            return None
    except ImportError:
        pass

    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    return parse_date_arg(text[:10])


def resolve_incremental_date_range(
    *,
    start_date_raw: str | None,
    end_date_raw: str | None,
    latest_trade_date: date | None,
    init_start_date: str = "2005-01-01",
) -> tuple[date, date]:
    """Resolve download range the same way DolphinDB daily ingest does."""
    today = date.today()

    if start_date_raw and end_date_raw:
        start_date = parse_date_arg(start_date_raw)
        end_date = parse_date_arg(end_date_raw)
    elif start_date_raw or end_date_raw:
        raise ValueError("Please provide both --start-date and --end-date, or provide neither")
    else:
        if latest_trade_date is None:
            start_date = parse_date_arg(init_start_date)
        else:
            start_date = latest_trade_date + timedelta(days=1)
        end_date = today

    if start_date > end_date:
        if latest_trade_date is not None and not start_date_raw and not end_date_raw:
            return start_date, end_date
        raise ValueError(f"Invalid date range: {start_date} > {end_date}")

    if end_date > today:
        end_date = today

    return start_date, end_date


def resolve_hfq_update_range(
    *,
    start_date_raw: str | None,
    end_date_raw: str | None,
    src_min_date: date | None,
    src_max_date: date | None,
    dst_max_date: date | None,
    lookback_days: int = 30,
) -> tuple[date, date]:
    """
    Align with DolphinDB HFQ rebuild:
    default incremental mode rewinds lookback_days to catch retroactive factor fixes.
    """
    if src_min_date is None or src_max_date is None:
        raise RuntimeError("Source table has no data")

    today = date.today()

    if start_date_raw and end_date_raw:
        start_date = parse_date_arg(start_date_raw)
        end_date = parse_date_arg(end_date_raw)
    elif start_date_raw or end_date_raw:
        raise ValueError("Please provide both --start-date and --end-date, or provide neither")
    else:
        if dst_max_date is None:
            start_date = src_min_date
        else:
            start_date = max(src_min_date, dst_max_date - timedelta(days=lookback_days))
        end_date = today

    if end_date > today:
        end_date = today
    if end_date > src_max_date:
        end_date = src_max_date
    if start_date < src_min_date:
        start_date = src_min_date

    return start_date, end_date
