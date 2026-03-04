SELECT
    event_hour,
    COUNT(*) AS event_count,
    COUNT(DISTINCT session_id) AS sessions,
    COUNT(DISTINCT user_email) AS users
FROM events
GROUP BY event_hour
ORDER BY event_count DESC, event_hour ASC
