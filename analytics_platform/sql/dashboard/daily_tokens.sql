SELECT
    e.event_date,
    COALESCE(emp.practice, e.resource_practice, 'Unknown') AS practice,
    COALESCE(SUM(e.input_tokens), 0) + COALESCE(SUM(e.output_tokens), 0) AS total_tokens
FROM events e
LEFT JOIN employees emp ON emp.email = e.user_email
{where_clause}
GROUP BY e.event_date, practice
ORDER BY e.event_date ASC, total_tokens DESC
