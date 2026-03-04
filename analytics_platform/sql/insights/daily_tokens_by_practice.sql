WITH bounded AS (
    SELECT
        e.event_date,
        e.input_tokens,
        e.output_tokens,
        emp.practice AS employee_practice,
        e.resource_practice
    FROM events e
    LEFT JOIN employees emp ON emp.email = e.user_email
    WHERE e.event_date >= DATE((SELECT MAX(event_date) FROM events), '-' || ? || ' days')
)
SELECT
    event_date,
    COALESCE(employee_practice, resource_practice, 'Unknown') AS practice,
    COALESCE(SUM(input_tokens), 0) AS input_tokens,
    COALESCE(SUM(output_tokens), 0) AS output_tokens,
    COALESCE(SUM(input_tokens), 0) + COALESCE(SUM(output_tokens), 0) AS total_tokens
FROM bounded
GROUP BY event_date, practice
ORDER BY event_date ASC, total_tokens DESC
