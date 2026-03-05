SELECT
    e.event_date,
    {group_select} AS group_value,
    COALESCE(SUM(e.input_tokens), 0) AS input_tokens,
    COALESCE(SUM(e.output_tokens), 0) AS output_tokens,
    COALESCE(SUM(e.input_tokens), 0) + COALESCE(SUM(e.output_tokens), 0) AS total_tokens,
    COUNT(*) AS event_count,
    ROUND(COALESCE(SUM(e.cost_usd), 0), 4) AS total_cost_usd
FROM events e
LEFT JOIN employees emp ON emp.email = e.user_email
{where_clause}
GROUP BY e.event_date, group_value
ORDER BY e.event_date ASC, total_tokens DESC
