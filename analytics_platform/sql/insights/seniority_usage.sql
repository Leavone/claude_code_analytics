SELECT
    COALESCE(emp.level, 'Unknown') AS level,
    COUNT(*) AS events,
    COUNT(DISTINCT e.session_id) AS sessions,
    COUNT(DISTINCT e.user_email) AS users,
    COALESCE(SUM(e.input_tokens), 0) AS input_tokens,
    COALESCE(SUM(e.output_tokens), 0) AS output_tokens,
    COALESCE(SUM(e.input_tokens), 0) + COALESCE(SUM(e.output_tokens), 0) AS total_tokens,
    ROUND(COALESCE(SUM(e.cost_usd), 0), 6) AS total_cost_usd,
    ROUND(
        CASE
            WHEN COUNT(DISTINCT e.session_id) = 0 THEN 0
            ELSE (COALESCE(SUM(e.input_tokens), 0) + COALESCE(SUM(e.output_tokens), 0)) * 1.0 / COUNT(DISTINCT e.session_id)
        END,
        2
    ) AS avg_tokens_per_session
FROM events e
LEFT JOIN employees emp ON emp.email = e.user_email
GROUP BY COALESCE(emp.level, 'Unknown')
ORDER BY total_tokens DESC
