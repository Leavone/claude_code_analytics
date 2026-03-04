SELECT
    e.model,
    COUNT(*) AS requests,
    ROUND(COALESCE(SUM(e.cost_usd), 0), 4) AS total_cost_usd,
    COALESCE(SUM(e.input_tokens), 0) AS input_tokens,
    COALESCE(SUM(e.output_tokens), 0) AS output_tokens
FROM events e
LEFT JOIN employees emp ON emp.email = e.user_email
{where_clause}
GROUP BY e.model
ORDER BY total_cost_usd DESC, requests DESC
