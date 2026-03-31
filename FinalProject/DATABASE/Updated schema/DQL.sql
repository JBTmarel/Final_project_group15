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
-- frá Evu:
SELECT
    s.name AS power_plant_source,
    EXTRACT(YEAR FROM w.timestamp) AS year,
    EXTRACT(MONTH FROM w.timestamp) AS month,
    c.name AS customer_name,
    SUM(w.value_kwh) AS total_kwh
FROM raforka_updated.withdraws_from w
JOIN raforka_updated.customer c
    ON c.id = w.customer_id
JOIN raforka_updated.station s
    ON s.id = w.power_plant_source_id
WHERE w.timestamp >= '2025-01-01'
  AND w.timestamp < '2026-01-01'
GROUP BY
    s.name,
    c.name,
    EXTRACT(YEAR FROM w.timestamp),
    EXTRACT(MONTH FROM w.timestamp)
ORDER BY
    power_plant_source,
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

-- Task F

-- ============================================================
-- Task F: Substation Flow Estimation
-- Cleaner version with:
-- 1) distance update included
-- 2) explicit substation lookup in writeback WHERE
-- 3) max_capacity_mw based on peak hourly flow proxy
-- ============================================================

-- ------------------------------------------------------------
-- Step 1: Calculate and store distances between connected substations
-- ------------------------------------------------------------
UPDATE raforka_updated.connects_to ct
SET distance = calc.distance_km
FROM (
    SELECT
        ct2.from_substation_id,
        ct2.to_substation_id,
        6371.0 * 2 * ASIN(
            SQRT(
                POWER(SIN(RADIANS(s2.y_coordinates - s1.y_coordinates) / 2), 2)
                + COS(RADIANS(s1.y_coordinates))
                * COS(RADIANS(s2.y_coordinates))
                * POWER(SIN(RADIANS(s2.x_coordinates - s1.x_coordinates) / 2), 2)
            )
        ) AS distance_km
    FROM raforka_updated.connects_to ct2
    JOIN raforka_updated.station s1
      ON s1.id = ct2.from_substation_id
    JOIN raforka_updated.station s2
      ON s2.id = ct2.to_substation_id
) calc
WHERE ct.from_substation_id = calc.from_substation_id
  AND ct.to_substation_id   = calc.to_substation_id;


-- ------------------------------------------------------------
-- Step 2: Flow estimation query for a date range
-- Replace literals with API params later
-- ------------------------------------------------------------
WITH
s1_injection AS (
    SELECT SUM(i.value_kwh) AS total_kwh
    FROM raforka_updated.injects_to i
    JOIN raforka_updated.substation sub
      ON i.substation_id = sub.substation_id
    JOIN raforka_updated.station s
      ON sub.substation_id = s.id
    WHERE s.name = 'S1_Krókur'
      AND i.timestamp >= '2025-01-01'
      AND i.timestamp <  '2026-01-01'
),
s2_injection AS (
    SELECT SUM(i.value_kwh) AS total_kwh
    FROM raforka_updated.injects_to i
    JOIN raforka_updated.substation sub
      ON i.substation_id = sub.substation_id
    JOIN raforka_updated.station s
      ON sub.substation_id = s.id
    WHERE s.name = 'S2_Rimakot'
      AND i.timestamp >= '2025-01-01'
      AND i.timestamp <  '2026-01-01'
),
s3_withdrawal AS (
    SELECT SUM(w.value_kwh) AS total_kwh
    FROM raforka_updated.withdraws_from w
    JOIN raforka_updated.substation sub
      ON w.substation_id = sub.substation_id
    JOIN raforka_updated.station s
      ON sub.substation_id = s.id
    WHERE s.name = 'S3_Vestmannaeyjar'
      AND w.timestamp >= '2025-01-01'
      AND w.timestamp <  '2026-01-01'
),
distances AS (
    SELECT
        MAX(CASE WHEN sf.name = 'S1_Krókur'  THEN ct.distance END) AS d_s1_s2,
        MAX(CASE WHEN sf.name = 'S2_Rimakot' THEN ct.distance END) AS d_s2_s3
    FROM raforka_updated.connects_to ct
    JOIN raforka_updated.substation sub_f
      ON ct.from_substation_id = sub_f.substation_id
    JOIN raforka_updated.station sf
      ON sub_f.substation_id = sf.id
),
flow AS (
    SELECT
        s1.total_kwh AS injected_s1,
        s2.total_kwh AS injected_s2,
        s3.total_kwh AS withdrawn_s3,
        (s1.total_kwh + s2.total_kwh) - s3.total_kwh AS total_system_loss,
        d.d_s1_s2,
        d.d_s2_s3,
        d.d_s1_s2 + d.d_s2_s3 AS total_distance,
        ((s1.total_kwh + s2.total_kwh) - s3.total_kwh)
            * (d.d_s1_s2 / (d.d_s1_s2 + d.d_s2_s3)) AS loss_s1_s2,
        ((s1.total_kwh + s2.total_kwh) - s3.total_kwh)
            * (d.d_s2_s3 / (d.d_s1_s2 + d.d_s2_s3)) AS loss_s2_s3
    FROM s1_injection s1
    CROSS JOIN s2_injection s2
    CROSS JOIN s3_withdrawal s3
    CROSS JOIN distances d
)
SELECT
    'S1_Krókur -> S2_Rimakot' AS segment,
    ROUND(d_s1_s2::NUMERIC, 2) AS distance_km,
    ROUND(injected_s1::NUMERIC, 2) AS flow_in_kwh,
    ROUND(loss_s1_s2::NUMERIC, 2) AS loss_kwh,
    ROUND((injected_s1 - loss_s1_s2)::NUMERIC, 2) AS flow_out_kwh,
    ROUND((loss_s1_s2 / NULLIF(injected_s1, 0) * 100)::NUMERIC, 4) AS loss_pct,
    ROUND(((injected_s1 - loss_s1_s2) / NULLIF(injected_s1, 0) * 100)::NUMERIC, 4) AS efficiency_pct
FROM flow

UNION ALL

SELECT
    'S2_Rimakot -> S3_Vestmannaeyjar' AS segment,
    ROUND(d_s2_s3::NUMERIC, 2) AS distance_km,
    ROUND((injected_s1 - loss_s1_s2 + injected_s2)::NUMERIC, 2) AS flow_in_kwh,
    ROUND(loss_s2_s3::NUMERIC, 2) AS loss_kwh,
    ROUND(withdrawn_s3::NUMERIC, 2) AS flow_out_kwh,
    ROUND((loss_s2_s3 / NULLIF((injected_s1 - loss_s1_s2 + injected_s2), 0) * 100)::NUMERIC, 4) AS loss_pct,
    ROUND((withdrawn_s3 / NULLIF((injected_s1 - loss_s1_s2 + injected_s2), 0) * 100)::NUMERIC, 4) AS efficiency_pct
FROM flow

UNION ALL

SELECT
    'TOTAL SYSTEM' AS segment,
    ROUND((d_s1_s2 + d_s2_s3)::NUMERIC, 2) AS distance_km,
    ROUND((injected_s1 + injected_s2)::NUMERIC, 2) AS flow_in_kwh,
    ROUND(total_system_loss::NUMERIC, 2) AS loss_kwh,
    ROUND(withdrawn_s3::NUMERIC, 2) AS flow_out_kwh,
    ROUND((total_system_loss / NULLIF((injected_s1 + injected_s2), 0) * 100)::NUMERIC, 4) AS loss_pct,
    ROUND((withdrawn_s3 / NULLIF((injected_s1 + injected_s2), 0) * 100)::NUMERIC, 4) AS efficiency_pct
FROM flow;


-- ------------------------------------------------------------
-- Step 3: Write back estimated flows
-- Uses peak hourly flow proxy for max_capacity_mw
-- ------------------------------------------------------------

BEGIN;

-- S1 -> S2
WITH flow AS (
    SELECT
        s1.total_kwh AS injected_s1,
        s2.total_kwh AS injected_s2,
        s3.total_kwh AS withdrawn_s3,
        ((s1.total_kwh + s2.total_kwh) - s3.total_kwh)
            * (d.d_s1_s2 / (d.d_s1_s2 + d.d_s2_s3)) AS loss_s1_s2,
        ((s1.total_kwh + s2.total_kwh) - s3.total_kwh)
            * (d.d_s2_s3 / (d.d_s1_s2 + d.d_s2_s3)) AS loss_s2_s3,
        d.d_s1_s2,
        d.d_s2_s3
    FROM
        (SELECT SUM(i.value_kwh) AS total_kwh
         FROM raforka_updated.injects_to i
         JOIN raforka_updated.substation sub
           ON i.substation_id = sub.substation_id
         JOIN raforka_updated.station s
           ON sub.substation_id = s.id
         WHERE s.name = 'S1_Krókur'
           AND i.timestamp >= '2025-01-01'
           AND i.timestamp <  '2026-01-01') s1,
        (SELECT SUM(i.value_kwh) AS total_kwh
         FROM raforka_updated.injects_to i
         JOIN raforka_updated.substation sub
           ON i.substation_id = sub.substation_id
         JOIN raforka_updated.station s
           ON sub.substation_id = s.id
         WHERE s.name = 'S2_Rimakot'
           AND i.timestamp >= '2025-01-01'
           AND i.timestamp <  '2026-01-01') s2,
        (SELECT SUM(w.value_kwh) AS total_kwh
         FROM raforka_updated.withdraws_from w
         JOIN raforka_updated.substation sub
           ON w.substation_id = sub.substation_id
         JOIN raforka_updated.station s
           ON sub.substation_id = s.id
         WHERE s.name = 'S3_Vestmannaeyjar'
           AND w.timestamp >= '2025-01-01'
           AND w.timestamp <  '2026-01-01') s3,
        (SELECT
             MAX(CASE WHEN sf.name = 'S1_Krókur'  THEN ct.distance END) AS d_s1_s2,
             MAX(CASE WHEN sf.name = 'S2_Rimakot' THEN ct.distance END) AS d_s2_s3
         FROM raforka_updated.connects_to ct
         JOIN raforka_updated.substation sub_f
           ON ct.from_substation_id = sub_f.substation_id
         JOIN raforka_updated.station sf
           ON sub_f.substation_id = sf.id) d
),
peak_capacity AS (
    SELECT MAX(hourly_flow_kwh) / 1000.0 AS peak_mw
    FROM (
        SELECT
            i.timestamp,
            SUM(i.value_kwh) AS hourly_flow_kwh
        FROM raforka_updated.injects_to i
        JOIN raforka_updated.substation sub
          ON i.substation_id = sub.substation_id
        JOIN raforka_updated.station s
          ON sub.substation_id = s.id
        WHERE s.name = 'S1_Krókur'
          AND i.timestamp >= '2025-01-01'
          AND i.timestamp <  '2026-01-01'
        GROUP BY i.timestamp
    ) hourly
)
UPDATE raforka_updated.connects_to ct
SET
    value_kwh = f.injected_s1 - f.loss_s1_s2,
    max_capacity_mw = pc.peak_mw
FROM flow f
CROSS JOIN peak_capacity pc
WHERE ct.from_substation_id = (
        SELECT sub.substation_id
        FROM raforka_updated.substation sub
        JOIN raforka_updated.station s
          ON sub.substation_id = s.id
        WHERE s.name = 'S1_Krókur'
      )
  AND ct.to_substation_id = (
        SELECT sub.substation_id
        FROM raforka_updated.substation sub
        JOIN raforka_updated.station s
          ON sub.substation_id = s.id
        WHERE s.name = 'S2_Rimakot'
      );


-- S2 -> S3
WITH flow AS (
    SELECT
        s1.total_kwh AS injected_s1,
        s2.total_kwh AS injected_s2,
        s3.total_kwh AS withdrawn_s3,
        ((s1.total_kwh + s2.total_kwh) - s3.total_kwh)
            * (d.d_s1_s2 / (d.d_s1_s2 + d.d_s2_s3)) AS loss_s1_s2,
        ((s1.total_kwh + s2.total_kwh) - s3.total_kwh)
            * (d.d_s2_s3 / (d.d_s1_s2 + d.d_s2_s3)) AS loss_s2_s3,
        d.d_s1_s2,
        d.d_s2_s3
    FROM
        (SELECT SUM(i.value_kwh) AS total_kwh
         FROM raforka_updated.injects_to i
         JOIN raforka_updated.substation sub
           ON i.substation_id = sub.substation_id
         JOIN raforka_updated.station s
           ON sub.substation_id = s.id
         WHERE s.name = 'S1_Krókur'
           AND i.timestamp >= '2025-01-01'
           AND i.timestamp <  '2026-01-01') s1,
        (SELECT SUM(i.value_kwh) AS total_kwh
         FROM raforka_updated.injects_to i
         JOIN raforka_updated.substation sub
           ON i.substation_id = sub.substation_id
         JOIN raforka_updated.station s
           ON sub.substation_id = s.id
         WHERE s.name = 'S2_Rimakot'
           AND i.timestamp >= '2025-01-01'
           AND i.timestamp <  '2026-01-01') s2,
        (SELECT SUM(w.value_kwh) AS total_kwh
         FROM raforka_updated.withdraws_from w
         JOIN raforka_updated.substation sub
           ON w.substation_id = sub.substation_id
         JOIN raforka_updated.station s
           ON sub.substation_id = s.id
         WHERE s.name = 'S3_Vestmannaeyjar'
           AND w.timestamp >= '2025-01-01'
           AND w.timestamp <  '2026-01-01') s3,
        (SELECT
             MAX(CASE WHEN sf.name = 'S1_Krókur'  THEN ct.distance END) AS d_s1_s2,
             MAX(CASE WHEN sf.name = 'S2_Rimakot' THEN ct.distance END) AS d_s2_s3
         FROM raforka_updated.connects_to ct
         JOIN raforka_updated.substation sub_f
           ON ct.from_substation_id = sub_f.substation_id
         JOIN raforka_updated.station sf
           ON sub_f.substation_id = sf.id) d
),
peak_capacity AS (
    SELECT MAX(hourly_flow_kwh) / 1000.0 AS peak_mw
    FROM (
        SELECT
            i.timestamp,
            SUM(i.value_kwh) AS hourly_flow_kwh
        FROM raforka_updated.injects_to i
        JOIN raforka_updated.substation sub
          ON i.substation_id = sub.substation_id
        JOIN raforka_updated.station s
          ON sub.substation_id = s.id
        WHERE s.name IN ('S1_Krókur', 'S2_Rimakot')
          AND i.timestamp >= '2025-01-01'
          AND i.timestamp <  '2026-01-01'
        GROUP BY i.timestamp
    ) hourly
)
UPDATE raforka_updated.connects_to ct
SET
    value_kwh = f.withdrawn_s3,
    max_capacity_mw = pc.peak_mw
FROM flow f
CROSS JOIN peak_capacity pc
WHERE ct.from_substation_id = (
        SELECT sub.substation_id
        FROM raforka_updated.substation sub
        JOIN raforka_updated.station s
          ON sub.substation_id = s.id
        WHERE s.name = 'S2_Rimakot'
      )
  AND ct.to_substation_id = (
        SELECT sub.substation_id
        FROM raforka_updated.substation sub
        JOIN raforka_updated.station s
          ON sub.substation_id = s.id
        WHERE s.name = 'S3_Vestmannaeyjar'
      );

-- Check results before commit
SELECT
    sf.name AS from_station,
    st.name AS to_station,
    ct.distance,
    ct.value_kwh,
    ct.max_capacity_mw
FROM raforka_updated.connects_to ct
JOIN raforka_updated.station sf
  ON sf.id = ct.from_substation_id
JOIN raforka_updated.station st
  ON st.id = ct.to_substation_id
ORDER BY sf.name, st.name;

-- If it looks good:
COMMIT;

-- If not:
-- ROLLBACK;