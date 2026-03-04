SELECT
    COUNT(*) AS total_events,
    COUNT(DISTINCT session_id) AS total_sessions,
    COUNT(DISTINCT user_email) AS total_users,
    ROUND(COALESCE(SUM(cost_usd), 0), 6) AS total_cost_usd,
    COALESCE(SUM(input_tokens), 0) AS total_input_tokens,
    COALESCE(SUM(output_tokens), 0) AS total_output_tokens,
    MIN(event_timestamp) AS first_event_at,
    MAX(event_timestamp) AS last_event_at
FROM events
