



SELECT COUNT(*)
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.STREET_GRID_DATA
;


SELECT * 
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.STREET_GRID_DATA
;


-- DELETE FROM LUMISENSE_DB.LUMISENSE_SCHEMA.STREET_GRID_DATA;





-- Verify
SELECT
    hour_of_day,
    COUNT(*)       AS readings,
    ROUND(AVG(ldr_value)) AS avg_ldr,
    MIN(ldr_value) AS min_ldr,
    MAX(ldr_value) AS max_ldr
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.street_grid_data
GROUP BY hour_of_day
ORDER BY hour_of_day;
























