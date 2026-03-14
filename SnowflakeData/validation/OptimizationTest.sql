-- ═══════════════════════════════════════════════════════════════
-- LUMISENSE GRID — OPTIMIZATION PROCEDURE TEST SUITE
-- Run each TEST block independently
-- Check EXPECTED OUTCOME after each CALL
-- ═══════════════════════════════════════════════════════════════


-- ───────────────────────────────────────────────────────────────
-- SETUP: Snapshot current rules before any test
-- Run this ONCE before starting tests
-- ───────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES_BACKUP
AS SELECT * FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES;

-- Confirm backup created
SELECT COUNT(*) AS "Rules Backed Up"
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES_BACKUP;


-- ───────────────────────────────────────────────────────────────
-- HELPER: Clear test data between runs
-- Run before EACH test to start clean
-- ───────────────────────────────────────────────────────────────
DELETE FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
WHERE light_id = 'TEST';


-- ───────────────────────────────────────────────────────────────
-- HELPER: Restore rules to original state between tests
-- Run if a test changed the rules and you want to reset
-- ───────────────────────────────────────────────────────────────
-- UPDATE LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES r
-- SET brightness_pct = b.brightness_pct,
--     saving_pct     = b.saving_pct,
--     timestamp      = b.timestamp
-- FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES_BACKUP b
-- WHERE r.scenario_id = b.scenario_id
--   AND r.light_id    = b.light_id;


-- ═══════════════════════════════════════════════════════════════
-- TEST 1 — BRIGHT DAYTIME → AI SHOULD REDUCE BRIGHTNESS
--
-- Scenario 13: LDR 600-1023, Hour 9-17, Daytime
-- Current brightness: 5%
-- We inject: high LDR readings (750-900) during daytime hours
-- Expected: Cortex sees bright environment, may reduce 5% → 4% or 3%
--           (floor for S13 = 5%, so only reduces if above floor)
--
-- NOTE: S13 brightness is already very low (5%), so Cortex may
--       confirm no change needed. That is also a valid outcome.
-- ═══════════════════════════════════════════════════════════════

-- Step 1: Clear old test data
DELETE FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA WHERE light_id = 'TEST';

-- Step 2: Inject 30 bright daytime readings (well above MIN_READINGS_REQUIRED = 15)
INSERT INTO LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
    (light_id, ldr_value, hour_of_day, timestamp)
SELECT
    'TEST',
    750 + (SEQ4() % 150),           -- LDR 750–900 (bright daylight)
    9   + (SEQ4() % 8),             -- Hours 9–17 (daytime)
    DATEADD(minute, -(SEQ4() * 20), CURRENT_TIMESTAMP())
FROM TABLE(GENERATOR(ROWCOUNT => 30));

-- Step 3: Check what we inserted
SELECT
    MIN(ldr_value)  AS min_ldr,
    MAX(ldr_value)  AS max_ldr,
    ROUND(AVG(ldr_value), 0) AS avg_ldr,
    COUNT(*)        AS total_readings,
    MIN(hour_of_day) AS hour_min,
    MAX(hour_of_day) AS hour_max
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
WHERE light_id = 'TEST';

-- Step 4: Check current S13 rule BEFORE
SELECT scenario_id, brightness_pct, saving_pct,
       TO_CHAR(timestamp, 'YYYY-MM-DD HH24:MI') AS last_updated
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES
WHERE scenario_id = 13 AND light_id = 'ALL';

-- Step 5: Run the procedure
CALL LUMISENSE_DB.LUMISENSE_SCHEMA.SP_OPTIMIZE_SCENARIO_RULES();

-- Step 6: Check S13 AFTER — did brightness change?
SELECT scenario_id, brightness_pct, saving_pct,
       TO_CHAR(timestamp, 'YYYY-MM-DD HH24:MI') AS last_updated
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES
WHERE scenario_id = 13 AND light_id = 'ALL';

-- Step 7: Check the optimization log
SELECT TO_CHAR(timestamp, 'YYYY-MM-DD HH24:MI') AS when_changed,
       scenario_id, old_brightness, new_brightness,
       old_saving, new_saving, reason
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_OPTIMIZATION_LOG
ORDER BY timestamp DESC LIMIT 5;

-- EXPECTED OUTCOME:
--   S13 brightness reduced (5→4 or 5→3) OR confirmed no change (already at floor)
--   Log entry shows "bright daylight" in reason


-- ═══════════════════════════════════════════════════════════════
-- TEST 2 — DARK EVENING → AI SHOULD INCREASE BRIGHTNESS
--
-- Scenario 9: LDR 300-599, Hour 18-21, Evening
-- Current brightness: 80%
-- We inject: LOW LDR readings (300-350) during peak evening hours
-- Expected: Cortex sees dark environment during peak hours
--           Increases brightness 80% → up to 85-90%
--           (ceiling for S9 = 100%, floor = 50%)
-- ═══════════════════════════════════════════════════════════════

-- Step 1: Clear old test data
DELETE FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA WHERE light_id = 'TEST';

-- Step 2: Restore rules to original
UPDATE LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES r
SET brightness_pct = b.brightness_pct,
    saving_pct     = b.saving_pct
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES_BACKUP b
WHERE r.scenario_id = b.scenario_id AND r.light_id = b.light_id;

-- Step 3: Inject 30 dark evening readings
INSERT INTO LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
    (light_id, ldr_value, hour_of_day, timestamp)
SELECT
    'TEST',
    300 + (SEQ4() % 50),            -- LDR 300–350 (barely moderate, very dim)
    18  + (SEQ4() % 3),             -- Hours 18–21 (peak evening)
    DATEADD(minute, -(SEQ4() * 20), CURRENT_TIMESTAMP())
FROM TABLE(GENERATOR(ROWCOUNT => 30));

-- Step 4: Check current S9 BEFORE
SELECT scenario_id, brightness_pct, saving_pct
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES
WHERE scenario_id = 9 AND light_id = 'ALL';

-- Step 5: Run the procedure
CALL LUMISENSE_DB.LUMISENSE_SCHEMA.SP_OPTIMIZE_SCENARIO_RULES();

-- Step 6: Check S9 AFTER
SELECT scenario_id, brightness_pct, saving_pct,
       TO_CHAR(timestamp, 'YYYY-MM-DD HH24:MI') AS last_updated
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES
WHERE scenario_id = 9 AND light_id = 'ALL';

-- Step 7: Check log
SELECT TO_CHAR(timestamp, 'YYYY-MM-DD HH24:MI') AS when_changed,
       scenario_id, old_brightness, new_brightness,
       CASE WHEN new_brightness > old_brightness THEN 'INCREASED'
            WHEN new_brightness < old_brightness THEN 'REDUCED'
            ELSE 'NO CHANGE' END AS direction,
       reason
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_OPTIMIZATION_LOG
ORDER BY timestamp DESC LIMIT 5;

-- EXPECTED OUTCOME:
--   S9 brightness increased (80→85 or 80→90, capped at +10%)
--   Log direction = INCREASED
--   Reason mentions "dark", "dim", "evening", "safety"


-- ═══════════════════════════════════════════════════════════════
-- TEST 3 — SPARSE DATA → AI SHOULD SKIP (GUARDRAIL TEST)
--
-- Scenario 6: LDR 300-599, Hour 0-5, Midnight
-- We inject only 5 readings (below MIN_READINGS_REQUIRED = 15)
-- Expected: Procedure skips S6 — not enough data to trust
-- ═══════════════════════════════════════════════════════════════

-- Step 1: Clear test data
DELETE FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA WHERE light_id = 'TEST';

-- Step 2: Inject only 5 readings (insufficient)
INSERT INTO LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
    (light_id, ldr_value, hour_of_day, timestamp)
SELECT
    'TEST',
    350 + (SEQ4() % 100),           -- LDR 350–450
    1   + (SEQ4() % 4),             -- Hours 1–4
    DATEADD(minute, -(SEQ4() * 60), CURRENT_TIMESTAMP())
FROM TABLE(GENERATOR(ROWCOUNT => 5));  -- Only 5 readings!

-- Step 3: Confirm only 5 readings exist
SELECT COUNT(*) AS "Readings Inserted (should be 5)"
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
WHERE light_id = 'TEST';

-- Step 4: Note S6 brightness BEFORE
SELECT scenario_id, brightness_pct AS brightness_before
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES
WHERE scenario_id = 6 AND light_id = 'ALL';

-- Step 5: Run the procedure
CALL LUMISENSE_DB.LUMISENSE_SCHEMA.SP_OPTIMIZE_SCENARIO_RULES();

-- Step 6: Check S6 AFTER — should be UNCHANGED
SELECT scenario_id, brightness_pct AS brightness_after
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES
WHERE scenario_id = 6 AND light_id = 'ALL';

-- EXPECTED OUTCOME:
--   S6 brightness_pct = SAME as before
--   Procedure log shows "SKIP — only 5 readings (need 15)"
--   No new row in optimization_log for S6


-- ═══════════════════════════════════════════════════════════════
-- TEST 4 — MAX CHANGE CAP → AI SUGGESTS BIG JUMP, SHOULD BE CAPPED
--
-- Scenario 2: LDR 0-299, Hour 6-8, Morning
-- Current brightness: 95%
-- We inject: very dark morning readings to push Cortex to suggest big increase
-- Even if Cortex says 100%, guardrail caps change at +10% → max 100% (or ceiling)
-- ═══════════════════════════════════════════════════════════════

-- Step 1: Clear test data
DELETE FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA WHERE light_id = 'TEST';

-- Step 2: Restore rules
UPDATE LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES r
SET brightness_pct = b.brightness_pct,
    saving_pct     = b.saving_pct
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES_BACKUP b
WHERE r.scenario_id = b.scenario_id AND r.light_id = b.light_id;

-- Step 3: Manually set S2 to 70% so a big jump is possible
UPDATE LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES
SET brightness_pct = 70
WHERE scenario_id = 2 AND light_id = 'ALL';

SELECT * FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA;

-- Step 4: Inject very dark morning readings
INSERT INTO LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
    (light_id, ldr_value, hour_of_day, timestamp)
SELECT
    'TEST',
    10  + (SEQ4() % 50),            -- LDR 10–60 (pitch dark morning)
    6   + (SEQ4() % 2),             -- Hours 6–7
    DATEADD(minute, -(SEQ4() * 20), CURRENT_TIMESTAMP())
FROM TABLE(GENERATOR(ROWCOUNT => 30));

-- Step 5: Run the procedure
CALL LUMISENSE_DB.LUMISENSE_SCHEMA.SP_OPTIMIZE_SCENARIO_RULES();

-- Step 6: Check S2 — should have increased but capped at +10% (70→80 max)
SELECT scenario_id, brightness_pct,
       70 AS was_set_to,
       brightness_pct - 70 AS actual_change,
       10 AS max_allowed_change
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES
WHERE scenario_id = 2 AND light_id = 'ALL';

-- EXPECTED OUTCOME:
--   brightness_pct = 80 (70 + 10 max cap)
--   actual_change = 10 (not more)
--   Log shows "CAPPED — change exceeded 10%"


-- ═══════════════════════════════════════════════════════════════
-- TEST 5 — FLOOR GUARDRAIL → AI TRIES TO GO BELOW SAFETY MINIMUM
--
-- Scenario 4: LDR 0-299, Hour 18-21, Evening (floor = 60%)
-- We set brightness to 65% and inject very bright readings
-- Cortex might suggest 50% (very bright evening, reduce aggressively)
-- Guardrail floor = 60% should block it
-- ═══════════════════════════════════════════════════════════════

-- Step 1: Clear test data
DELETE FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA WHERE light_id = 'TEST';

-- Step 2: Restore rules then set S4 to 65%
UPDATE LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES r
SET brightness_pct = b.brightness_pct,
    saving_pct     = b.saving_pct
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES_BACKUP b
WHERE r.scenario_id = b.scenario_id AND r.light_id = b.light_id;

UPDATE LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES
SET brightness_pct = 65
WHERE scenario_id = 4 AND light_id = 'ALL';

-- Step 3: Inject readings that look like a bright evening
-- LDR just barely in 0-299 range but hitting upper end
INSERT INTO LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
    (light_id, ldr_value, hour_of_day, timestamp)
SELECT
    'TEST',
    250 + (SEQ4() % 40),            -- LDR 250–290 (high for dark range)
    18  + (SEQ4() % 3),             -- Hours 18–21
    DATEADD(minute, -(SEQ4() * 20), CURRENT_TIMESTAMP())
FROM TABLE(GENERATOR(ROWCOUNT => 30));

-- Step 4: Check S4 BEFORE
SELECT scenario_id, brightness_pct AS brightness_before
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES
WHERE scenario_id = 4 AND light_id = 'ALL';

-- Step 5: Run the procedure
CALL LUMISENSE_DB.LUMISENSE_SCHEMA.SP_OPTIMIZE_SCENARIO_RULES();

-- Step 6: Check S4 AFTER — should not go below 60%
SELECT scenario_id,
       brightness_pct AS brightness_after,
       60 AS floor_guardrail,
       CASE WHEN brightness_pct >= 60 THEN 'GUARDRAIL HELD'
            ELSE 'GUARDRAIL FAILED' END AS result
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES
WHERE scenario_id = 4 AND light_id = 'ALL';

-- EXPECTED OUTCOME:
--   result = 'GUARDRAIL HELD'
--   brightness_pct >= 60 always
--   Log shows "SKIP — below safety floor 60%"


-- ═══════════════════════════════════════════════════════════════
-- TEST 6 — NO DATA AT ALL → PROCEDURE SHOULD HANDLE GRACEFULLY
--
-- Delete all recent data so no scenario has readings
-- Procedure should return "No changes needed" cleanly
-- ═══════════════════════════════════════════════════════════════

-- Step 1: Remove all recent data (last 24 hours)
DELETE FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
WHERE timestamp >= DATEADD(day, -1, CURRENT_TIMESTAMP());

-- Step 2: Confirm no recent readings exist
SELECT COUNT(*) AS "Recent Readings (should be 0)"
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
WHERE timestamp >= DATEADD(day, -1, CURRENT_TIMESTAMP());

-- Step 3: Run the procedure
CALL LUMISENSE_DB.LUMISENSE_SCHEMA.SP_OPTIMIZE_SCENARIO_RULES();

-- EXPECTED OUTCOME:
--   Procedure returns cleanly
--   Log shows all scenarios SKIPPED (0 readings each)
--   No rows inserted into optimization_log
--   No rules updated


-- ═══════════════════════════════════════════════════════════════
-- FINAL: RESTORE EVERYTHING TO ORIGINAL STATE
-- Run this after all tests are complete
-- ═══════════════════════════════════════════════════════════════

-- Restore all rules
UPDATE LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES r
SET brightness_pct = b.brightness_pct,
    saving_pct     = b.saving_pct,
    timestamp      = b.timestamp
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES_BACKUP b
WHERE r.scenario_id = b.scenario_id
  AND r.light_id    = b.light_id;

-- Remove test sensor data
DELETE FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
WHERE light_id = 'TEST';

-- Remove test entries from optimization log
DELETE FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_OPTIMIZATION_LOG
WHERE timestamp >= DATEADD(hour, -2, CURRENT_TIMESTAMP());

-- Confirm restore
SELECT scenario_id, brightness_pct, saving_pct
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES
WHERE light_id = 'ALL'
ORDER BY scenario_id;