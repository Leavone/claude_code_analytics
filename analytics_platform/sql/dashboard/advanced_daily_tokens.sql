SELECT
    e.event_date,
    SUM(COALESCE(e.input_tokens, 0) + COALESCE(e.output_tokens, 0)) AS total_tokens,
    COUNT(*) AS api_requests,
    COUNT(DISTINCT e.session_id) AS sessions
FROM events_enriched e
LEFT JOIN employees emp ON emp.email = e.user_email
{where_clause}
GROUP BY e.event_date
ORDER BY e.event_date ASC;
