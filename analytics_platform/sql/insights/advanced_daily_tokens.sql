WITH bounds AS (
    SELECT MAX(event_date) AS max_date
    FROM events_enriched
    WHERE event_name = 'api_request'
)
SELECT
    e.event_date,
    COALESCE(SUM(e.input_tokens), 0) AS input_tokens,
    COALESCE(SUM(e.output_tokens), 0) AS output_tokens,
    SUM(COALESCE(e.input_tokens, 0) + COALESCE(e.output_tokens, 0)) AS total_tokens,
    COUNT(*) AS event_count,
    ROUND(COALESCE(SUM(e.cost_usd), 0), 4) AS total_cost_usd,
    COUNT(*) AS api_requests,
    COUNT(DISTINCT e.session_id) AS sessions
FROM events_enriched e
WHERE e.event_name = 'api_request'
  AND e.event_date >= DATE((SELECT max_date FROM bounds), '-' || (? - 1) || ' days')
GROUP BY e.event_date
ORDER BY e.event_date ASC;
