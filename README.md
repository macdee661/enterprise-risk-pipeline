# Enterprise Risk Pipeline

A small Python and SQLite project for loading trade data, validating records, quarantining invalid data, and analyzing counterparty exposure.

## Project layout

- `data/generate_data.py` creates the SQLite database and sample batch feed.
- `src/validator.py` validates each trade record.
- `src/pipeline.py` processes the feed and writes trades, quarantined records, and a batch audit entry.
- `sql/queries.sql` contains SQL analytics for deduplicated daily exposures and risk metrics.
- `database/risk_data.db` is the generated SQLite database.
- `batch_feed.json` is the generated sample input.

## Run locally

Run these commands from the project root in PowerShell:

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
python data\generate_data.py
python src\pipeline.py
```

The project uses Python's standard library and SQLite; no third-party packages are required.

The generator recreates the database and writes the sample feed. The sample feed contains invalid records; if more than 15% of a batch is invalid, the pipeline's circuit breaker records the invalid rows and does not commit valid trades from that batch.

## Query the data

Open `database/risk_data.db` with a SQLite client such as DB Browser for SQLite, or run the statements in `sql/queries.sql` against it.
