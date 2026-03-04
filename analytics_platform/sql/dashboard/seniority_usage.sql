SELECT
    COALESCE(emp.level, 'Unknown') AS level,
    COUNT(*) AS events,
    COUNT(DISTINCT e.session_id) AS sessions,
    COALESCE(SUM(e.input_tokens), 0) + COALESCE(SUM(e.output_tokens), 0) AS total_tokens,
    ROUND(COALESCE(SUM(e.cost_usd), 0), 4) AS total_cost_usd
FROM events e
LEFT JOIN employees emp ON emp.email = e.user_email
{where_clause}
GROUP BY COALESCE(emp.level, 'Unknown')
ORDER BY total_tokens DESC
