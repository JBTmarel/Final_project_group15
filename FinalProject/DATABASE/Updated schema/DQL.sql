-- Task C4:

-- Query 1 - Frá Claude, mætti lesa yfir
WITH combined_measurements AS (
    -- Production (Framleiðsla)
    SELECT
        s.name AS power_plant_source,
        'Framleiðsla' AS measurement_type,
        EXTRACT(YEAR FROM p.timestamp)::INTEGER AS year,
        EXTRACT(MONTH FROM p.timestamp)::INTEGER AS month,
        p.value_kwh
    FROM raforka_updated.production p
    JOIN raforka_updated.station s ON p.power_plant_id = s.id

    UNION ALL

    -- Injection (Innmötun)
    SELECT
        s.name AS power_plant_source,
        'Innmötun' AS measurement_type,
        EXTRACT(YEAR FROM i.timestamp)::INTEGER AS year,
        EXTRACT(MONTH FROM i.timestamp)::INTEGER AS month,
        i.value_kwh
    FROM raforka_updated.injects_to i
    JOIN raforka_updated.station s ON i.power_plant_id = s.id

    UNION ALL

    -- Withdrawal (Úttekt)
    -- Join withdraws_from to injects_to via substation_id and timestamp
    -- to trace back to the power plant
    SELECT
        s.name AS power_plant_source,
        'Úttekt' AS measurement_type,
        EXTRACT(YEAR FROM w.timestamp)::INTEGER AS year,
        EXTRACT(MONTH FROM w.timestamp)::INTEGER AS month,
        w.value_kwh
    FROM raforka_updated.withdraws_from w
    JOIN raforka_updated.injects_to i 
        ON w.substation_id = i.substation_id
        AND w.timestamp = i.timestamp
    JOIN raforka_updated.station s ON i.power_plant_id = s.id
)
SELECT
    power_plant_source,
    measurement_type,
    year,
    month,
    SUM(value_kwh) AS total_kwh
FROM combined_measurements
WHERE year = 2025
GROUP BY power_plant_source, measurement_type, year, month
ORDER BY power_plant_source, month ASC, total_kwh DESC;


-- Query 2:
-- aftur sama ves með stafrófsröð
SELECT
    EXTRACT(YEAR FROM w.timestamp) AS year,
    EXTRACT(MONTH FROM w.timestamp) AS month,
    c.name AS customer_name,
    SUM(w.value_kwh) AS total_kwh
FROM raforka_updated.withdraws_from w
JOIN raforka_updated.customer c
    ON c.id = w.customer_id
WHERE w.timestamp >= '2025-01-01'
  AND w.timestamp < '2026-01-01'
GROUP BY
    EXTRACT(YEAR FROM w.timestamp),
    EXTRACT(MONTH FROM w.timestamp),
    c.name
ORDER BY
    month ASC,
    customer_name ASC;


-- Query 3:


