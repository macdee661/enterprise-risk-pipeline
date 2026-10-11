import sqlite3
import json
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "database" / "risk_data.db"
FEED_PATH = "batch_feed.json"


def init_database():
    """Initializes schema and seeds historical data for Part 1 SQL analytics."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    if DB_PATH.exists():
        DB_PATH.unlink()

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # 1. Base Tables
    cur.executescript("""
    CREATE TABLE counterparty_limits (
        counterparty_id TEXT PRIMARY KEY,
        internal_rating TEXT NOT NULL,
        max_exposure_limit REAL NOT NULL,
        effective_from TEXT NOT NULL
    );

    CREATE TABLE trades_snapshot (
        trade_id TEXT NOT NULL,
        counterparty_id TEXT NOT NULL,
        book_id TEXT NOT NULL,
        asset_class TEXT NOT NULL,
        notional_usd REAL NOT NULL,
        mtm_value REAL NOT NULL,
        snapshot_timestamp TEXT NOT NULL,
        version INTEGER NOT NULL
    );

    -- Staging / Pipeline Execution Tables for Part 2
    CREATE TABLE quarantine_records (
        quarantine_id INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_id TEXT NOT NULL,
        raw_payload TEXT NOT NULL,
        error_reason TEXT NOT NULL,
        quarantined_at TEXT NOT NULL
    );

    CREATE TABLE ingestion_audit_log (
        batch_id TEXT PRIMARY KEY,
        execution_time_ms REAL NOT NULL,
        records_received INTEGER NOT NULL,
        records_committed INTEGER NOT NULL,
        records_quarantined INTEGER NOT NULL,
        total_mtm_usd REAL NOT NULL,
        status TEXT NOT NULL,
        completed_at TEXT NOT NULL
    );
    """)

    # 2. Seed Counterparty Limits
    limits = [
        ("CP_01", "AA", 45000000.00, "2026-01-01"),
        ("CP_02", "BBB", 20000000.00, "2026-01-01"),
        ("CP_03", "BB", 10000000.00, "2026-01-01")  # High-risk counterparty
    ]
    cur.executemany("INSERT INTO counterparty_limits VALUES (?, ?, ?, ?)", limits)

    # 3. Seed Multi-Day Trades for Part 1 SQL Testing
    # Includes multiple versions on the same day and multi-day exposure changes
    historical_trades = [
        # --- Date: 2026-10-01 ---
        ("T_101", "CP_01", "RATES_US", "RATES", 15000000.0, 1200000.0, "2026-10-01 16:00:00", 1),
        ("T_102", "CP_02", "FX_G10", "FX", 8000000.0, -350000.0, "2026-10-01 16:00:00", 1),
        ("T_103", "CP_03", "CRED_HY", "CREDIT", 7500000.0, 8500000.0, "2026-10-01 16:00:00", 1),

        # --- Date: 2026-10-02 (T_101 has Version 1 & 2 update; CP_03 breaches 10M limit) ---
        ("T_101", "CP_01", "RATES_US", "RATES", 15000000.0, 1100000.0, "2026-10-02 09:15:00", 1),
        ("T_101", "CP_01", "RATES_US", "RATES", 15000000.0, 1450000.0, "2026-10-02 16:30:00", 2), # Latest version
        ("T_102", "CP_02", "FX_G10", "FX", 8000000.0, -100000.0, "2026-10-02 16:00:00", 1),
        ("T_103", "CP_03", "CRED_HY", "CREDIT", 7500000.0, 10500000.0, "2026-10-02 16:00:00", 1), # Breach #1

        # --- Date: 2026-10-03 (CP_03 breaches second day in a row) ---
        ("T_101", "CP_01", "RATES_US", "RATES", 15000000.0, 1300000.0, "2026-10-03 16:00:00", 1),
        ("T_102", "CP_02", "FX_G10", "FX", 9000000.0, 400000.0, "2026-10-03 16:00:00", 1),
        ("T_103", "CP_03", "CRED_HY", "CREDIT", 7500000.0, 11800000.0, "2026-10-03 16:00:00", 1), # Breach #2

        # --- Date: 2026-10-04 (CP_03 drops back under limit) ---
        ("T_101", "CP_01", "RATES_US", "RATES", 15000000.0, 1250000.0, "2026-10-04 16:00:00", 1),
        ("T_102", "CP_02", "FX_G10", "FX", 9000000.0, 650000.0, "2026-10-04 16:00:00", 1),
        ("T_103", "CP_03", "CRED_HY", "CREDIT", 7500000.0, 9200000.0, "2026-10-04 16:00:00", 1),

        # --- Date: 2026-10-05 (Collision test: same version, pick later timestamp) ---
        ("T_101", "CP_01", "RATES_US", "RATES", 15000000.0, 950000.0, "2026-10-05 10:00:00", 1),
        ("T_101", "CP_01", "RATES_US", "RATES", 15000000.0, 1050000.0, "2026-10-05 17:00:00", 1), # Pick this
        ("T_102", "CP_02", "FX_G10", "FX", 10000000.0, 800000.0, "2026-10-05 16:00:00", 1),
        ("T_103", "CP_03", "CRED_HY", "CREDIT", 8000000.0, 12100000.0, "2026-10-05 16:00:00", 1)  # Breach #1
    ]

    cur.executemany("INSERT INTO trades_snapshot VALUES (?, ?, ?, ?, ?, ?, ?, ?)", historical_trades)
    conn.commit()
    conn.close()
    print(f" Database '{DB_PATH}' seeded successfully with {len(historical_trades)} historical trades.")


def generate_incoming_batch_json():
    """Generates the dirty 20-record JSON payload for testing pipeline.py."""
    dirty_batch = [
        # 14 Valid Records
        {"trade_id": "T_201", "counterparty_id": "CP_01", "book_id": "RATES_1", "asset_class": "RATES", "notional_usd": 12000000.0, "mtm_value": 450000.0, "snapshot_timestamp": "2026-10-06 16:00:00", "version": 1},
        {"trade_id": "T_202", "counterparty_id": "CP_01", "book_id": "RATES_2", "asset_class": "RATES", "notional_usd": 5000000.0, "mtm_value": 120000.0, "snapshot_timestamp": "2026-10-06 16:00:00", "version": 1},
        {"trade_id": "T_203", "counterparty_id": "CP_02", "book_id": "FX_SPOT", "asset_class": "FX", "notional_usd": 3500000.0, "mtm_value": -85000.0, "snapshot_timestamp": "2026-10-06 16:00:00", "version": 1},
        {"trade_id": "T_204", "counterparty_id": "CP_02", "book_id": "FX_FWD", "asset_class": "FX", "notional_usd": 4100000.0, "mtm_value": 310000.0, "snapshot_timestamp": "2026-10-06 16:00:00", "version": 1},
        {"trade_id": "T_205", "counterparty_id": "CP_03", "book_id": "CR_CDS", "asset_class": "CREDIT", "notional_usd": 8000000.0, "mtm_value": 1150000.0, "snapshot_timestamp": "2026-10-06 16:00:00", "version": 1},
        {"trade_id": "T_206", "counterparty_id": "CP_01", "book_id": "EQ_DESK", "asset_class": "EQUITIES", "notional_usd": 2500000.0, "mtm_value": 60000.0, "snapshot_timestamp": "2026-10-06 16:00:00", "version": 1},
        {"trade_id": "T_207", "counterparty_id": "CP_02", "book_id": "FX_FWD", "asset_class": "FX", "notional_usd": 1500000.0, "mtm_value": 45000.0, "snapshot_timestamp": "2026-10-06 16:00:00", "version": 1},
        {"trade_id": "T_208", "counterparty_id": "CP_03", "book_id": "CR_CDS", "asset_class": "CREDIT", "notional_usd": 3200000.0, "mtm_value": 290000.0, "snapshot_timestamp": "2026-10-06 16:00:00", "version": 1},
        {"trade_id": "T_209", "counterparty_id": "CP_01", "book_id": "RATES_1", "asset_class": "RATES", "notional_usd": 9000000.0, "mtm_value": -150000.0, "snapshot_timestamp": "2026-10-06 16:00:00", "version": 1},
        {"trade_id": "T_210", "counterparty_id": "CP_02", "book_id": "FX_SPOT", "asset_class": "FX", "notional_usd": 6700000.0, "mtm_value": 820000.0, "snapshot_timestamp": "2026-10-06 16:00:00", "version": 1},
        {"trade_id": "T_211", "counterparty_id": "CP_03", "book_id": "EQ_DESK", "asset_class": "EQUITIES", "notional_usd": 1800000.0, "mtm_value": -30000.0, "snapshot_timestamp": "2026-10-06 16:00:00", "version": 1},
        {"trade_id": "T_212", "counterparty_id": "CP_01", "book_id": "RATES_2", "asset_class": "RATES", "notional_usd": 11000000.0, "mtm_value": 410000.0, "snapshot_timestamp": "2026-10-06 16:00:00", "version": 1},
        {"trade_id": "T_213", "counterparty_id": "CP_02", "book_id": "FX_FWD", "asset_class": "FX", "notional_usd": 2200000.0, "mtm_value": 75000.0, "snapshot_timestamp": "2026-10-06 16:00:00", "version": 1},
        {"trade_id": "T_214", "counterparty_id": "CP_03", "book_id": "CR_CDS", "asset_class": "CREDIT", "notional_usd": 4000000.0, "mtm_value": 310000.0, "snapshot_timestamp": "2026-10-06 16:00:00", "version": 1},

        # 2 Corrupted: Missing/Null counterparty_id
        {"trade_id": "T_215", "counterparty_id": None, "book_id": "RATES_1", "asset_class": "RATES", "notional_usd": 1000000.0, "mtm_value": 15000.0, "snapshot_timestamp": "2026-10-06 16:00:00", "version": 1},
        {"trade_id": "T_216", "counterparty_id": "", "book_id": "FX_SPOT", "asset_class": "FX", "notional_usd": 2000000.0, "mtm_value": 25000.0, "snapshot_timestamp": "2026-10-06 16:00:00", "version": 1},

        # 2 Corrupted: Invalid notionals (negative / non-numeric string)
        {"trade_id": "T_217", "counterparty_id": "CP_01", "book_id": "EQ_DESK", "asset_class": "EQUITIES", "notional_usd": -5000000.0, "mtm_value": -10000.0, "snapshot_timestamp": "2026-10-06 16:00:00", "version": 1},
        {"trade_id": "T_218", "counterparty_id": "CP_02", "book_id": "CR_CDS", "asset_class": "CREDIT", "notional_usd": "10.5M", "mtm_value": 50000.0, "snapshot_timestamp": "2026-10-06 16:00:00", "version": 1},

        # 2 Duplicates: Collision on (trade_id, version) within same batch
        {"trade_id": "T_219", "counterparty_id": "CP_03", "book_id": "RATES_1", "asset_class": "RATES", "notional_usd": 3000000.0, "mtm_value": 80000.0, "snapshot_timestamp": "2026-10-06 16:00:00", "version": 1},
        {"trade_id": "T_219", "counterparty_id": "CP_03", "book_id": "RATES_1", "asset_class": "RATES", "notional_usd": 3000000.0, "mtm_value": 80000.0, "snapshot_timestamp": "2026-10-06 16:00:00", "version": 1}
    ]

    with open(FEED_PATH, "w") as f:
        json.dump(dirty_batch, f, indent=2)

    print(f" Generated dirty batch feed '{FEED_PATH}' with {len(dirty_batch)} records.")


if __name__ == "__main__":
    init_database()
    generate_incoming_batch_json()