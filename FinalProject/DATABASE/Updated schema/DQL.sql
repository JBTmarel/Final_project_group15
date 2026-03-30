-- Task C4:

-- Query 1 
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
-- VIEWS
CREATE OR REPLACE VIEW raforka_updated.v_monthly_plant_energy AS
WITH monthly_production AS (
    SELECT
        p.power_plant_id,
        EXTRACT(YEAR FROM p.timestamp)::int AS year,
        EXTRACT(MONTH FROM p.timestamp)::int AS month,
        SUM(p.value_kwh) AS production_kwh
    FROM raforka_updated.production p
    WHERE p.timestamp >= '2025-01-01'
      AND p.timestamp <  '2026-01-01'
    GROUP BY p.power_plant_id,
             EXTRACT(YEAR FROM p.timestamp),
             EXTRACT(MONTH FROM p.timestamp)
),
monthly_injection AS (
    SELECT
        i.power_plant_id,
        EXTRACT(YEAR FROM i.timestamp)::int AS year,
        EXTRACT(MONTH FROM i.timestamp)::int AS month,
        SUM(i.value_kwh) AS injection_kwh
    FROM raforka_updated.injects_to i
    WHERE i.timestamp >= '2025-01-01'
      AND i.timestamp <  '2026-01-01'
    GROUP BY i.power_plant_id,
             EXTRACT(YEAR FROM i.timestamp),
             EXTRACT(MONTH FROM i.timestamp)
),
monthly_total_withdrawal AS (
    SELECT
        EXTRACT(YEAR FROM w.timestamp)::int AS year,
        EXTRACT(MONTH FROM w.timestamp)::int AS month,
        SUM(w.value_kwh) AS total_withdrawal_kwh
    FROM raforka_updated.withdraws_from w
    WHERE w.timestamp >= '2025-01-01'
      AND w.timestamp <  '2026-01-01'
    GROUP BY EXTRACT(YEAR FROM w.timestamp),
             EXTRACT(MONTH FROM w.timestamp)
),
monthly_total_injection AS (
    SELECT
        mi.year,
        mi.month,
        SUM(mi.injection_kwh) AS total_injection_kwh
    FROM monthly_injection mi
    GROUP BY mi.year, mi.month
)
SELECT
    mp.power_plant_id,
    s.name AS power_plant_source,
    mp.year,
    mp.month,
    mp.production_kwh,
    COALESCE(mi.injection_kwh, 0) AS injection_kwh,
    COALESCE(
        mtw.total_withdrawal_kwh
        * COALESCE(mi.injection_kwh, 0)
        / NULLIF(mti.total_injection_kwh, 0),
        0
    ) AS attributed_withdrawal_kwh
FROM monthly_production mp
JOIN raforka_updated.power_plant pp
  ON pp.power_plant_id = mp.power_plant_id
JOIN raforka_updated.station s
  ON s.id = pp.power_plant_id
JOIN monthly_injection mi
  ON mi.power_plant_id = mp.power_plant_id
 AND mi.year = mp.year
 AND mi.month = mp.month
JOIN monthly_total_withdrawal mtw
  ON mtw.year = mp.year
 AND mtw.month = mp.month
JOIN monthly_total_injection mti
  ON mti.year = mp.year
 AND mti.month = mp.month;

 --MAIN QUERY
 SELECT
    power_plant_source,
    AVG(
        (production_kwh - injection_kwh) / NULLIF(production_kwh, 0)
    ) AS plant_to_substation_loss_ratio,
    AVG(
        (production_kwh - attributed_withdrawal_kwh) / NULLIF(production_kwh, 0)
    ) AS total_system_loss_ratio
FROM raforka_updated.v_monthly_plant_energy
GROUP BY power_plant_source
ORDER BY power_plant_source;