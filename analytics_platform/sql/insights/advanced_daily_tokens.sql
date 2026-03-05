WITH bounds AS (
    SELECT MAX(event_date) AS max_date
    FROM events_enriched
    WHERE event_name = 'api_request'
)
SELECT
    e.event_date,
    SUM(COALESCE(e.input_tokens, 0) + COALESCE(e.output_tokens, 0)) AS total_tokens,
    COUNT(*) AS api_requests,
    COUNT(DISTINCT e.session_id) AS sessions
FROM events_enriched e
WHERE e.event_name = 'api_request'
  AND e.event_date >= DATE((SELECT max_date FROM bounds), '-' || (? - 1) || ' days')
GROUP BY e.event_date
ORDER BY e.event_date ASC;
