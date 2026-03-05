SELECT
    COALESCE(employee_practice, resource_practice, 'Unknown') AS practice,
    session_id,
    SUM(COALESCE(input_tokens, 0) + COALESCE(output_tokens, 0)) AS session_tokens,
    COUNT(*) AS api_requests,
    SUM(COALESCE(cost_usd, 0.0)) AS total_cost_usd
FROM events_enriched
WHERE event_name = 'api_request'
  AND session_id IS NOT NULL
GROUP BY 1, 2
HAVING SUM(COALESCE(input_tokens, 0) + COALESCE(output_tokens, 0)) > 0
ORDER BY session_tokens DESC;
