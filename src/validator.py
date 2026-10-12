ALLOWED_ASSET_CLASSES = {"RATES", "FX", "CREDIT", "EQUITIES"}

def validate_record(record: dict) -> tuple[bool, str]:
    """
    Inspects a raw trade payload.
    Returns:
        (True, "") if the record satisfies all rules.
        (False, error_reason) on the first rule failure.
    """
    if record.get("asset_class") not in ALLOWED_ASSET_CLASSES:
        return (False, "Invalid Asset Class")

    trade_id = record.get("trade_id")
    if not isinstance(trade_id, str) or not trade_id.strip():
        return (False, "trade_id should be a non-empty string")

    cp_id = record.get("counterparty_id")
    if not isinstance(cp_id, str) or not cp_id.strip():
        return (False, "counterparty_id should be a non-empty string")

    try:
        notional_usd = float(record.get("notional_usd"))
    except (ValueError, TypeError):
        return (False, "notional_usd should be a number castable to float")
    
    if notional_usd <= 0.00:
        return (False, "notional_usd should be greater than 0.00")

    try:
        float(record.get("mtm_value"))
    except (ValueError, TypeError):
        return (False, "mtm_value should be a number castable to float")

    return (True, "")