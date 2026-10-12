
from pathlib import Path
import sqlite3
import json
from validator import validate_record
from datetime import datetime, timezone
import time

DB_PATH = Path("database/risk_data.db")
FEED_PATH = Path("batch_feed.json")

def run_pipeline(batch_id, feed_path=FEED_PATH, db_path=DB_PATH):

    start_time = time.perf_counter()

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("SELECT status from ingestion_audit_log WHERE batch_id = ?", (batch_id,))
    existing_run = cur.fetchone()

    if existing_run and existing_run[0] == "SUCCESS":
        conn.close()
        return "SKIPPED_ALREADY_PROCESSED"

    feed_file = open(feed_path, "r")
    batch_feed = json.load(feed_file)

    valid_records = []
    quarantine_records = []
    total_mtm_usd = 0

    for record in batch_feed:
        valid, message = validate_record(record)

        if valid:
            valid_records.append(record)
            total_mtm_usd += record.get("mtm_value")
        else:
            quarantine_records.append(
                {
                    "batch_id": batch_id,
                    "raw_payload": json.dumps(record),
                    "error_reason": message,
                    "quarantined_at": datetime.now(timezone.utc).isoformat()
                }
            )


    error_rate = len(quarantine_records) / len(batch_feed)

    if error_rate > .15:

        with conn:

            query = """
                INSERT INTO quarantine_records (
                    "batch_id", "raw_payload", "error_reason", "quarantined_at"
                ) VALUES (:batch_id, :raw_payload, :error_reason, :quarantined_at)
            """

            cur.executemany(query, quarantine_records)

            execution_time = time.perf_counter() - start_time
            audit = {
                "batch_id": batch_id,
                "execution_time_ms": round(execution_time * 1000, 2),
                "records_received": len(batch_feed),
                "records_committed": 0,
                "records_quarantined": len(quarantine_records),
                "total_mtm_usd": 0,
                "status": "FAILED_CIRCUIT_BREAKER",
                "completed_at" : datetime.now(timezone.utc).isoformat()
            }
            query = """
                INSERT OR REPLACE INTO ingestion_audit_log (
                    batch_id, execution_time_ms, records_received, records_committed,
                    records_quarantined, total_mtm_usd, status, completed_at
                ) VALUES (
                    :batch_id, :execution_time_ms, :records_received, :records_committed,
                    :records_quarantined, :total_mtm_usd, :status, :completed_at
                )
            """
            cur.execute(query, audit)
        conn.close()
        return "FAILED_CURCUIT_BREAKER"

    else:
        with conn:
            
            query = """
                INSERT INTO trades_snapshot (
                    "TRADE_ID", "COUNTERPARTY_ID", "BOOK_ID", "ASSET_CLASS", 
                    "NOTIONAL_USD", "MTM_VALUE", "SNAPSHOT_TIMESTAMP", "VERSION"
                ) VALUES VALUES (:trade_id, :counterparty_id, :book_id, :asset_class, :notional_usd, :mtm_value, :snapshot_timestamp, :version)
            """
            cur.executemany(query, valid_records)

            query = """
                INSERT INTO quarantine_records (
                    "batch_id", "raw_payload", "error_reason", "quarantined_at"
                ) VALUES (:batch_id, :raw_payload, :error_reason, :quarantined_at)
            """

            cur.executemany(query, quarantine_records)

            execution_time = time.perf_counter() - start_time
            audit = {
                "batch_id": batch_id,
                "execution_time_ms": round(execution_time * 1000, 2),
                "records_received": len(batch_feed),
                "records_committed": len(valid_records),
                "records_quarantined": len(quarantine_records),
                "total_mtm_usd": total_mtm_usd,
                "status": "SUCCESS",
                "completed_at" : str(time.now())
            }
            query = """
                INSERT OR REPLACE INTO ingestion_audit_log (
                    batch_id, execution_time_ms, records_received, records_committed,
                    records_quarantined, total_mtm_usd, status, completed_at
                ) VALUES (
                    :batch_id, :execution_time_ms, :records_received, :records_committed,
                    :records_quarantined, :total_mtm_usd, :status, :completed_at
                )
            """
            cur.execute(query, audit)

        conn.close()
        return "SUCCESS"
  
if __name__ == "__main__":
    result = run_pipeline("BATCH-20261006-01")
    print(f"Pipeline executed with result: {result}")