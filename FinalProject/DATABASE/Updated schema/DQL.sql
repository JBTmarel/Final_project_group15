WITH combined_measurements AS (
    -- 1. Production (Framleiðsla)
    -- Joins production to station to get the plant name
    SELECT 
        s.name AS power_plant_source,
        'Framleiðsla' AS measurement_type,
        EXTRACT(YEAR FROM p.timestamp)::INTEGER AS year,
        EXTRACT(MONTH FROM p.timestamp)::INTEGER AS month,
        p.value_kwh
    FROM raforka_updated.production p
    JOIN raforka_updated.station s ON p.power_plant_id = s.id

    UNION ALL

    -- 2. Injection (Innmötun)
    -- Joins injects_to to station to get the source plant name
    SELECT 
        s.name AS power_plant_source,
        'Innmötun' AS measurement_type,
        EXTRACT(YEAR FROM i.timestamp)::INTEGER AS year,
        EXTRACT(MONTH FROM i.timestamp)::INTEGER AS month,
        i.value_kwh
    FROM raforka_updated.injects_to i
    JOIN raforka_updated.station s ON i.power_plant_id = s.id

    UNION ALL

    -- 3. Withdrawal (Úttekt)
    -- In your new schema, withdrawals are technically linked to Substations.
    -- To attribute these to a specific plant for this A2 query, 
    -- we rely on the logic that we know which plant's energy is being traced.
    -- Note: If you didn't add a 'plant_name' column to 'withdraws_from', 
    -- this specific part requires the Part F flow logic or a temporary mapping.
    -- For now, we assume the relationship is tracked:
    SELECT 
        'P1_Þröstur' AS power_plant_source, -- Placeholder: Attribute based on your data logic
        'Úttekt' AS measurement_type,
        EXTRACT(YEAR FROM w.timestamp)::INTEGER AS year,
        EXTRACT(MONTH FROM w.timestamp)::INTEGER AS month,
        w.value_kwh
    FROM raforka_updated.withdraws_from w
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
ORDER BY power_plant_source, month, measurement_type;