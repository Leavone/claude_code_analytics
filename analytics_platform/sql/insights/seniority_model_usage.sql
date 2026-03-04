SELECT
    COALESCE(emp.level, 'Unknown') AS level,
    e.model,
    COUNT(*) AS requests,
    ROUND(COALESCE(SUM(e.cost_usd), 0), 6) AS total_cost_usd,
    COALESCE(SUM(e.input_tokens), 0) AS input_tokens,
    COALESCE(SUM(e.output_tokens), 0) AS output_tokens
FROM events e
LEFT JOIN employees emp ON emp.email = e.user_email
WHERE e.event_body = 'claude_code.api_request'
  AND e.model IS NOT NULL
GROUP BY COALESCE(emp.level, 'Unknown'), e.model
ORDER BY level ASC, total_cost_usd DESC, requests DESC
