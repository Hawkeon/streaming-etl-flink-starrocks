-- DAU/MAU Analysis
-- Daily Active Users vs Monthly Active Users ratio

-- Daily Active Users (DAU) - distinct users per day
SELECT
    DATE(ts) AS date,
    COUNT(DISTINCT user_id) AS dau
FROM music_db.fact_events
GROUP BY DATE(ts)
ORDER BY date;

-- Monthly Active Users (MAU) - distinct users per month
SELECT
    DATE_FORMAT(ts, '%Y-%m') AS month,
    COUNT(DISTINCT user_id) AS mau
FROM music_db.fact_events
GROUP BY DATE_FORMAT(ts, '%Y-%m')
ORDER BY month;

-- DAU/MAU Ratio (stickiness metric)
-- Higher ratio = more engaged users
SELECT
    dau.month,
    dau.date,
    dau.dau,
    mau.mau,
    ROUND(dau.dau * 100.0 / mau.mau, 2) AS dau_mau_ratio_pct
FROM (
    SELECT DATE(ts) AS date, COUNT(DISTINCT user_id) AS dau
    FROM music_db.fact_events
    GROUP BY DATE(ts)
) dau
JOIN (
    SELECT DATE_FORMAT(ts, '%Y-%m-01') AS month, COUNT(DISTINCT user_id) AS mau
    FROM music_db.fact_events
    GROUP BY DATE_FORMAT(ts, '%Y-%m')
) mau ON DATE_FORMAT(dau.date, '%Y-%m') = DATE_FORMAT(mau.month, '%Y-%m')
ORDER BY dau.date;
