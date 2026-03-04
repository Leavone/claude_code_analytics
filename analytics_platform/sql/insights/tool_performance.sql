SELECT
    tool_name,
    COUNT(*) AS runs,
    ROUND(AVG(CASE
        WHEN success IS NULL THEN NULL
        WHEN success = 1 THEN 1.0
        ELSE 0.0
    END), 4) AS success_rate,
    ROUND(AVG(duration_ms), 2) AS avg_duration_ms,
    MAX(duration_ms) AS max_duration_ms
FROM events
WHERE event_body = 'claude_code.tool_result' AND tool_name IS NOT NULL
GROUP BY tool_name
HAVING COUNT(*) >= ?
ORDER BY runs DESC
