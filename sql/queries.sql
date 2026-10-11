WITH ranked_trades AS (
    SELECT 
        trade_id,
        counterparty_id,
        book_id,
        asset_class,
        notional_usd,
        mtm_value,
        snapshot_timestamp,
        version,
        DATE(snapshot_timestamp) AS trade_date,
        ROW_NUMBER() OVER (
            PARTITION BY trade_id, DATE(snapshot_timestamp)
            ORDER BY version DESC, snapshot_timestamp DESC
        ) AS rnk
    FROM trades_snapshot
),
filtered_trades AS (
    SELECT * 
    FROM ranked_trades 
    WHERE rnk = 1
),
daily_exposures AS (
    SELECT 
        t.trade_date,
        t.counterparty_id,
        l.internal_rating,
        l.max_exposure_limit,
        ROUND(SUM(t.mtm_value), 2) AS daily_net_exposure
    FROM filtered_trades t
    INNER JOIN counterparty_limits l 
        ON t.counterparty_id = l.counterparty_id
    GROUP BY 
        t.trade_date, 
        t.counterparty_id, 
        l.internal_rating, 
        l.max_exposure_limit
),
windowed_metrics AS (
    SELECT 
        trade_date,
        counterparty_id,
        internal_rating,
        max_exposure_limit,
        daily_net_exposure,
        
        -- 1. 3-Day Rolling Moving Average
        ROUND(AVG(daily_net_exposure) OVER (
            PARTITION BY counterparty_id 
            ORDER BY trade_date 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 2) AS rolling_3d_avg_exposure,

        -- 2. Day-over-Day (DoD) Dollar Delta
        ROUND(daily_net_exposure - LAG(daily_net_exposure, 1) OVER (
            PARTITION BY counterparty_id 
            ORDER BY trade_date
        ), 2) AS dod_exposure_delta,

        -- 3. Day-over-Day (DoD) Percentage Change
        ROUND(
            ((daily_net_exposure - LAG(daily_net_exposure, 1) OVER (
                PARTITION BY counterparty_id 
                ORDER BY trade_date
            )) / ABS(LAG(daily_net_exposure, 1) OVER (
                PARTITION BY counterparty_id 
                ORDER BY trade_date
            ))) * 100.0, 
            2
        ) AS dod_pct_change,

        -- 4. Limit Breach Flag
        CASE 
            WHEN daily_net_exposure > max_exposure_limit THEN 1 
            ELSE 0 
        END AS is_breached,

        -- 5. Island ID for Gaps-and-Islands streak calculation
        ROW_NUMBER() OVER (
            PARTITION BY counterparty_id 
            ORDER BY trade_date
        ) - ROW_NUMBER() OVER (
            PARTITION BY counterparty_id, (CASE WHEN daily_net_exposure > max_exposure_limit THEN 1 ELSE 0 END) 
            ORDER BY trade_date
        ) AS breach_island
    FROM daily_exposures
)
SELECT 
    trade_date,
    counterparty_id,
    internal_rating,
    max_exposure_limit,
    daily_net_exposure,
    rolling_3d_avg_exposure,
    dod_exposure_delta,
    dod_pct_change,
    is_breached,
    CASE 
        WHEN is_breached = 1 THEN 
            ROW_NUMBER() OVER (
                PARTITION BY counterparty_id, is_breached, breach_island 
                ORDER BY trade_date
            )
        ELSE 0 
    END AS consecutive_breach_days
FROM windowed_metrics
ORDER BY counterparty_id, trade_date;