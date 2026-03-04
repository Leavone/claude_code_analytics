SELECT
    model,
    COUNT(*) AS requests,
    ROUND(COALESCE(SUM(cost_usd), 0), 6) AS total_cost_usd,
    COALESCE(SUM(input_tokens), 0) AS input_tokens,
    COALESCE(SUM(output_tokens), 0) AS output_tokens
FROM events
WHERE event_body = 'claude_code.api_request' AND model IS NOT NULL
GROUP BY model
ORDER BY total_cost_usd DESC, requests DESC
