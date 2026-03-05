SELECT
    COALESCE(emp.practice, e.resource_practice, 'Unknown') AS practice,
    e.session_id,
    SUM(COALESCE(e.input_tokens, 0) + COALESCE(e.output_tokens, 0)) AS session_tokens,
    COUNT(*) AS api_requests,
    SUM(COALESCE(e.cost_usd, 0.0)) AS total_cost_usd
FROM events_enriched e
LEFT JOIN employees emp ON emp.email = e.user_email
{where_clause}
GROUP BY 1, 2
HAVING SUM(COALESCE(e.input_tokens, 0) + COALESCE(e.output_tokens, 0)) > 0
ORDER BY session_tokens DESC;
