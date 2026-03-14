








INSERT INTO LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES(

    scenario_id,
    light_id,
    ldr_min,
    ldr_max,
    hour_min,
    hour_max,
    brightness_pct,
    energy_mode,
    saving_pct
    
) VALUES
-- LDR 0-299 (DARK)
(1,  'ALL', 0,   299,  0,  5,  50,  'ECO',    50),
(2,  'ALL', 0,   299,  6,  8,  95,  'FULL',   5),
(3,  'ALL', 0,   299,  9,  17, 80,  'NORMAL', 20),
(4,  'ALL', 0,   299,  18, 21, 100, 'FULL',   0),
(5,  'ALL', 0,   299,  22, 23, 60,  'NORMAL', 40),
-- LDR 300-599 (MODERATE)
(6,  'ALL', 300, 599,  0,  5,  20,  'ECO',    80),
(7,  'ALL', 300, 599,  6,  8,  60,  'NORMAL', 40),
(8,  'ALL', 300, 599,  9,  17, 30,  'ECO',    70),
(9,  'ALL', 300, 599,  18, 21, 80,  'FULL',   20),
(10, 'ALL', 300, 599,  22, 23, 35,  'NORMAL', 65),
-- LDR 600-1023 (BRIGHT)
(11, 'ALL', 600, 1023, 0,  5,  10,  'ECO',    90),
(12, 'ALL', 600, 1023, 6,  8,  30,  'ECO',    70),
(13, 'ALL', 600, 1023, 9,  17, 5,   'ECO',    95),
(14, 'ALL', 600, 1023, 18, 21, 50,  'NORMAL', 50),
(15, 'ALL', 600, 1023, 22, 23, 15,  'ECO',    85);

-- Verify
-- SELECT * FROM LDR_LED_SCENARIO_RULES ORDER BY scenario_id;


SELECT * FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES;

-- DELETE FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES;

-- DROP TABLE LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES;