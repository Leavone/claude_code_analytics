SELECT
    COALESCE(employee_practice, resource_practice, 'Unknown') AS practice,
    session_id,
    SUM(
        CASE WHEN event_name = 'api_request'
            THEN COALESCE(input_tokens, 0) + COALESCE(output_tokens, 0)
            ELSE 0
        END
    ) AS session_tokens,
    SUM(CASE WHEN event_name = 'api_request' THEN 1 ELSE 0 END) AS api_requests,
    SUM(CASE WHEN event_name = 'api_request' THEN COALESCE(cost_usd, 0.0) ELSE 0.0 END) AS total_cost_usd,
    SUM(CASE WHEN event_name = 'api_request' THEN COALESCE(cache_read_tokens, 0) ELSE 0 END) AS cache_read_tokens,
    SUM(CASE WHEN event_name = 'api_request' THEN COALESCE(cache_creation_tokens, 0) ELSE 0 END)
        AS cache_creation_tokens,
    SUM(CASE WHEN event_name = 'tool_result' THEN 1 ELSE 0 END) AS tool_runs,
    SUM(CASE WHEN event_name = 'tool_result' AND success = 1 THEN 1 ELSE 0 END) AS successful_tool_runs
FROM events_enriched
WHERE session_id IS NOT NULL
GROUP BY 1, 2
HAVING SUM(CASE WHEN event_name = 'api_request' THEN 1 ELSE 0 END) > 0
ORDER BY session_tokens DESC;
