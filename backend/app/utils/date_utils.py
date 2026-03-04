from datetime import date


def holding_period_days(buy_date: date, as_of: date | None = None) -> int:
    as_of = as_of or date.today()
    return (as_of - buy_date).days
