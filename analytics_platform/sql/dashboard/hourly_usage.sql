SELECT
    e.event_hour,
    COUNT(*) AS event_count,
    COUNT(DISTINCT e.session_id) AS sessions
FROM events e
LEFT JOIN employees emp ON emp.email = e.user_email
{where_clause}
GROUP BY e.event_hour
ORDER BY event_count DESC
