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

