SELECT
    e.user_email,
    COALESCE(emp.practice, e.resource_practice, 'Unknown') AS practice,
    COALESCE(SUM(e.input_tokens), 0) + COALESCE(SUM(e.output_tokens), 0) AS total_tokens,
    ROUND(COALESCE(SUM(e.cost_usd), 0), 4) AS total_cost_usd
FROM events e
LEFT JOIN employees emp ON emp.email = e.user_email
{where_clause}
GROUP BY e.user_email, practice
ORDER BY total_tokens DESC
LIMIT ?
