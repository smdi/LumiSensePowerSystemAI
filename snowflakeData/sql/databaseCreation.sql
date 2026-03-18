



ALTER ACCOUNT SET TIMEZONE = 'Asia/Kolkata';

SELECT CURRENT_TIMESTAMP;



-- ── DATABASE & SCHEMA ─────────────────────────────────────────
CREATE DATABASE LUMISENSE_DB;
CREATE SCHEMA LUMISENSE_DB.LUMISENSE_SCHEMA;

-- ── TABLE 1: LDR_GRID_DATA (renamed from street_grid_data) ───
CREATE OR REPLACE TABLE LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA (
    timestamp   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    light_id    STRING,
    ldr_value   INT,
    hour_of_day INT
);

-- ── TABLE 2: LDR_LED_SCENARIO_RULES (renamed from brightness_rules) ──
CREATE OR REPLACE TABLE LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES (
    timestamp      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    scenario_id    INT,
    light_id       STRING,
    ldr_min        INT,
    ldr_max        INT,
    hour_min       INT,
    hour_max       INT,
    brightness_pct INT,
    energy_mode    STRING,
    saving_pct     INT
);

-- ── TABLE 3: LDR_LED_OPTIMIZATION_LOG (renamed from optimization_log) ──
CREATE OR REPLACE TABLE LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_OPTIMIZATION_LOG (
    timestamp      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    light_id       STRING,       -- 'ALL' = applies to all lights in scenario
    scenario_id    INT,
    old_brightness INT,
    new_brightness INT,
    old_saving     INT,
    new_saving     INT,
    reason         STRING
);


-- ── TABLE 4: Sensor health log table
CREATE OR REPLACE TABLE LUMISENSE_DB.LUMISENSE_SCHEMA.SENSOR_HEALTH_LOG (
    timestamp   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    light_id    STRING,
    fault_type  STRING,    -- DEAD / FROZEN / NOISY / SILENT / WRONG_PATTERN
    severity    STRING,    -- CRITICAL / WARNING / OK
    reason      STRING,
    action      STRING     -- what to physically check
);





