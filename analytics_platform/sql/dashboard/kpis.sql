SELECT
    COUNT(*) AS events,
    COUNT(DISTINCT e.session_id) AS sessions,
    COUNT(DISTINCT e.user_email) AS users,
    ROUND(COALESCE(SUM(e.cost_usd), 0), 4) AS total_cost_usd,
    COALESCE(SUM(e.input_tokens), 0) + COALESCE(SUM(e.output_tokens), 0) AS total_tokens
FROM events e
LEFT JOIN employees emp ON emp.email = e.user_email
{where_clause}
