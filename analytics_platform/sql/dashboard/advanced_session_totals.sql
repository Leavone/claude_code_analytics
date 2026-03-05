SELECT
    COALESCE(emp.practice, e.resource_practice, 'Unknown') AS practice,
    e.session_id,
    SUM(
        CASE WHEN e.event_name = 'api_request'
            THEN COALESCE(e.input_tokens, 0) + COALESCE(e.output_tokens, 0)
            ELSE 0
        END
    ) AS session_tokens,
    SUM(CASE WHEN e.event_name = 'api_request' THEN 1 ELSE 0 END) AS api_requests,
    SUM(CASE WHEN e.event_name = 'api_request' THEN COALESCE(e.cost_usd, 0.0) ELSE 0.0 END) AS total_cost_usd,
    SUM(CASE WHEN e.event_name = 'api_request' THEN COALESCE(e.cache_read_tokens, 0) ELSE 0 END) AS cache_read_tokens,
    SUM(CASE WHEN e.event_name = 'api_request' THEN COALESCE(e.cache_creation_tokens, 0) ELSE 0 END)
        AS cache_creation_tokens,
    SUM(CASE WHEN e.event_name = 'tool_result' THEN 1 ELSE 0 END) AS tool_runs,
    SUM(CASE WHEN e.event_name = 'tool_result' AND e.success = 1 THEN 1 ELSE 0 END) AS successful_tool_runs
FROM events_enriched e
LEFT JOIN employees emp ON emp.email = e.user_email
{where_clause}
GROUP BY 1, 2
HAVING SUM(CASE WHEN e.event_name = 'api_request' THEN 1 ELSE 0 END) > 0
ORDER BY session_tokens DESC;
