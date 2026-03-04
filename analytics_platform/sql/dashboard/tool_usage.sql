SELECT
    e.tool_name,
    COUNT(*) AS runs,
    ROUND(AVG(CASE
        WHEN e.success IS NULL THEN NULL
        WHEN e.success = 1 THEN 1.0
        ELSE 0.0
    END), 4) AS success_rate,
    ROUND(AVG(e.duration_ms), 2) AS avg_duration_ms
FROM events e
LEFT JOIN employees emp ON emp.email = e.user_email
{where_clause}
GROUP BY e.tool_name
ORDER BY runs DESC
LIMIT ?
