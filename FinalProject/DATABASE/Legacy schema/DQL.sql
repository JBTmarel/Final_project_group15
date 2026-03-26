-- Task A2


--Query 1:
SELECT eining_heiti power_plant_source,
    EXTRACT(year from timi) AS year,
    EXTRACT(month from timi) AS month,
    tegund_maelingar measurement_type,
    SUM(gildi_kwh) total_kwh
FROM raforka_legacy.orku_maelingar
GROUP BY eining_heiti, 
    EXTRACT(month from timi),
    EXTRACT(year from timi),
    tegund_maelingar
ORDER BY eining_heiti, month ASC, total_kwh DESC

--Query 2:
-- hér er röðun á customer name ekki rétt, ÍBV er röngum stað
SELECT 
    eining_heiti AS power_plant_source,
    EXTRACT(YEAR FROM timi) AS year,
    EXTRACT(MONTH FROM timi) AS month,
    notandi_heiti AS customer_name,
    SUM(gildi_kwh) AS total_kwh
FROM raforka_legacy.orku_maelingar
WHERE tegund_maelingar = 'Úttekt' 
    AND timi >= '2025-01-01' 
    AND timi < '2026-01-01'
GROUP BY
    eining_heiti,
    EXTRACT(YEAR FROM timi),
    EXTRACT(MONTH FROM timi),
    notandi_heiti
ORDER BY
    power_plant_source,
    month asc,
    customer_name asc;

-- Query 3:
-- View for montly aggregation
CREATE VIEW monthly_plant_totals AS
SELECT
    eining_heiti AS power_plant_source,
    EXTRACT(YEAR FROM timi) AS year,
    EXTRACT(MONTH FROM timi) AS month,
    tegund_maelingar,
    SUM(gildi_kwh) AS total_kwh
FROM raforka_legacy.orku_maelingar
GROUP BY eining_heiti, EXTRACT(YEAR FROM timi), EXTRACT(MONTH FROM timi), tegund_maelingar;

-- Main query using the view to calculate average loss ratios
SELECT
    f.power_plant_source,
    AVG( (f.total_kwh - i.total_kwh) / f.total_kwh ) AS plant_to_substation_loss_ratio,
    AVG( (f.total_kwh - u.total_kwh) / f.total_kwh ) AS total_system_loss_ratio
FROM monthly_plant_totals f
JOIN monthly_plant_totals i ON f.power_plant_source = i.power_plant_source 
    AND f.month = i.month 
    AND f.year = i.year
    AND i.tegund_maelingar = 'Innmötun'
JOIN monthly_plant_totals u ON f.power_plant_source = u.power_plant_source 
    AND f.month = u.month 
    AND f.year = u.year
    AND u.tegund_maelingar = 'Úttekt'
WHERE f.tegund_maelingar = 'Framleiðsla'
GROUP BY f.power_plant_source
ORDER BY f.power_plant_source;
