SELECT DISTINCT COALESCE(emp.practice, e.resource_practice, 'Unknown') AS practice
FROM events e
LEFT JOIN employees emp ON emp.email = e.user_email
ORDER BY practice ASC
