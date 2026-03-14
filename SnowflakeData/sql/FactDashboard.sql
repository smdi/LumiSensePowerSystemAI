


-- ═══════════════════════════════════════════════════════════════
-- STEP 1: CREATE FACT TABLE
-- Run this in Snowflake first
-- ═══════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE LUMISENSE_DB.LUMISENSE_SCHEMA.fact_dashboard (

    -- ── Identity ──────────────────────────────────────────────
    fact_id             VARCHAR,
    snapshot_time       TIMESTAMP,
    light_id            VARCHAR,
    zone                VARCHAR,        -- Residential / Main Road / Park / Industrial
    zone_type           VARCHAR,        -- Urban / Sub-Urban / Green / Commercial

    -- ── Sensor ────────────────────────────────────────────────
    ldr_value           INT,
    ldr_category        VARCHAR,        -- Dark / Moderate / Bright
    hour_of_day         INT,
    time_period         VARCHAR,        -- Midnight / Morning / Daytime / Evening / Late Night
    day_of_week         VARCHAR,        -- Monday - Sunday
    is_weekend          BOOLEAN,
    month_name          VARCHAR,

    -- ── Brightness Decision ───────────────────────────────────
    brightness_pct      INT,
    energy_mode         VARCHAR,        -- ECO / NORMAL / FULL
    saving_pct          INT,

    -- ── Power & Cost (Realistic mocked values) ────────────────
    wattage_per_light   FLOAT,          -- Standard 150W street light
    actual_wattage      FLOAT,          -- After dimming
    power_saved_w       FLOAT,          -- Watts saved this interval
    cost_per_kwh        FLOAT,          -- $0.12 per kWh
    cost_saved_usd      FLOAT,          -- USD saved this 15min interval
    co2_saved_kg        FLOAT,          -- CO2 saved (0.233kg per kWh)

    -- ── Grid Health ───────────────────────────────────────────
    voltage_v           FLOAT,          -- Simulated voltage 220-240V
    current_a           FLOAT,          -- Simulated current draw
    grid_load_pct       FLOAT,          -- % of max grid capacity used

    -- ── Scenario ──────────────────────────────────────────────
    scenario_id         INT,
    scenario_label      VARCHAR         -- human readable scenario
);



SELECT * FROM LUMISENSE_DB.LUMISENSE_SCHEMA.FACT_DASHBOARD;