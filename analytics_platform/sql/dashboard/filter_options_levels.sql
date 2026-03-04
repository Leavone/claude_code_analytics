SELECT DISTINCT COALESCE(level, 'Unknown') AS level
FROM employees
ORDER BY
    CASE
        WHEN level GLOB 'L[0-9]*' THEN 0
        ELSE 1
    END,
    CASE
        WHEN level GLOB 'L[0-9]*' THEN CAST(SUBSTR(level, 2) AS INTEGER)
        ELSE NULL
    END,
    level ASC
