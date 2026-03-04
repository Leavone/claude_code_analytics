SELECT DISTINCT COALESCE(level, 'Unknown') AS level
FROM employees
ORDER BY level ASC
