-- ═══════════════════════════════════════════════════════════════
-- LUMISENSE GRID — OPTIMIZATION PROCEDURE TEST SUITE
-- ═══════════════════════════════════════════════════════════════
--
-- IMPORTANT: The SP only reads light_id = 'ALL' from LDR_GRID_DATA
-- So all test data must be inserted as light_id = 'ALL'
--
-- Each test:
--   1. Backs up real LDR_GRID_DATA (last 24h)
--   2. Clears last 24h data
--   3. Injects controlled test data as light_id = 'ALL'
--   4. Runs SP
--   5. Checks result
--   6. Restores real data from backup
-- ═══════════════════════════════════════════════════════════════


-- ───────────────────────────────────────────────────────────────
-- STEP 1: BACKUP TABLE — Run ONCE before any test
-- Backs up both rules and recent LDR data
-- ───────────────────────────────────────────────────────────────

-- Backup rules
CREATE OR REPLACE TABLE LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES_BACKUP
AS SELECT * FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES;

SELECT COUNT(*) AS "Rules Backed Up"
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES_BACKUP;

-- Backup last 24h LDR data
CREATE OR REPLACE TABLE LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA_BACKUP
AS SELECT * FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
WHERE timestamp >= DATEADD(day, -1, CURRENT_TIMESTAMP());

SELECT COUNT(*) AS "LDR Readings Backed Up"
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA_BACKUP;


-- ───────────────────────────────────────────────────────────────
-- RESTORE HELPER — Run between tests to reset everything
-- ───────────────────────────────────────────────────────────────

-- Restore rules
UPDATE LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES r
SET brightness_pct = b.brightness_pct,
    saving_pct     = b.saving_pct,
    timestamp      = b.timestamp
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES_BACKUP b
WHERE r.scenario_id = b.scenario_id AND r.light_id = b.light_id;

-- Clear all last 24h LDR data (test + real)
DELETE FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
WHERE timestamp >= DATEADD(day, -1, CURRENT_TIMESTAMP());

-- Restore real LDR data from backup
INSERT INTO LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
SELECT * FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA_BACKUP;

SELECT 'Restored rules + LDR data' AS status;


-- ═══════════════════════════════════════════════════════════════
-- TEST 1 — BRIGHT DAYTIME → CORTEX SHOULD REDUCE BRIGHTNESS
--
-- Scenario 13 : LDR 600-1023, Hour 9-17
-- Inject      : 30 bright daytime readings (LDR 750-900)
-- Expected    : S13 reduces OR stays (already at floor = 5%)
-- ═══════════════════════════════════════════════════════════════

-- Step 1: Restore rules
-- UPDATE LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES r
-- SET brightness_pct = b.brightness_pct, saving_pct = b.saving_pct
-- FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES_BACKUP b
-- WHERE r.scenario_id = b.scenario_id AND r.light_id = b.light_id;

-- -- Step 2: Clear last 24h LDR data and inject test data
-- DELETE FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
-- WHERE timestamp >= DATEADD(day, -1, CURRENT_TIMESTAMP());

-- INSERT INTO LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
--     (light_id, ldr_value, hour_of_day, timestamp)
-- SELECT
--     'ALL',
--     750 + (SEQ4() % 150),           -- LDR 750-900 (bright daylight)
--     9   + (SEQ4() % 8),             -- Hours 9-17  (daytime)
--     DATEADD(minute, -(SEQ4() * 20), CURRENT_TIMESTAMP())
-- FROM TABLE(GENERATOR(ROWCOUNT => 30));

-- -- Step 3: Verify
-- SELECT MIN(ldr_value) AS min_ldr, MAX(ldr_value) AS max_ldr,
--        ROUND(AVG(ldr_value),0) AS avg_ldr, COUNT(*) AS readings
-- FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
-- WHERE timestamp >= DATEADD(day, -1, CURRENT_TIMESTAMP());

-- -- Step 4: Check S13 BEFORE
-- SELECT 'BEFORE' AS when_, scenario_id, brightness_pct, saving_pct
-- FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES
-- WHERE scenario_id = 13 AND light_id = 'ALL';

-- -- Step 5: Run SP
-- CALL LUMISENSE_DB.LUMISENSE_SCHEMA.SP_OPTIMIZE_SCENARIO_RULES();


-- -- 01c31ad3-3202-748c-0014-aeda0001dd92
-- SELECT LAST_QUERY_ID();

-- SELECT *
-- FROM TABLE(RESULT_SCAN('01c31ad3-3202-748c-0014-aeda0001dd92'));

-- -- Step 6: Check S13 AFTER
-- SELECT 'AFTER' AS when_, scenario_id, brightness_pct, saving_pct,
--        TO_CHAR(timestamp, 'YYYY-MM-DD HH24:MI') AS last_updated
-- FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES
-- WHERE scenario_id = 13 AND light_id = 'ALL';

-- -- Step 7: Check log
-- SELECT TO_CHAR(timestamp,'YYYY-MM-DD HH24:MI') AS when_,
--        scenario_id, old_brightness, new_brightness, reason
-- FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_OPTIMIZATION_LOG
-- ORDER BY timestamp DESC LIMIT 5;

-- -- Step 8: Restore real data
-- DELETE FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
-- WHERE timestamp >= DATEADD(day, -1, CURRENT_TIMESTAMP());
-- INSERT INTO LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
-- SELECT * FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA_BACKUP;

-- EXPECTED: S13 reduced 5→4 OR no change (at floor), reason mentions "bright daytime"


-- ═══════════════════════════════════════════════════════════════
-- TEST 2 — DARK EVENING → CORTEX SHOULD INCREASE BRIGHTNESS
--
-- Scenario 9  : LDR 300-599, Hour 18-21
-- Inject      : 30 dim evening readings (LDR 300-350)
-- Expected    : S9 increases 80% → 85-90% (capped at +10%)
-- ═══════════════════════════════════════════════════════════════

-- Step 1: Restore rules
UPDATE LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES r
SET brightness_pct = b.brightness_pct, saving_pct = b.saving_pct
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES_BACKUP b
WHERE r.scenario_id = b.scenario_id AND r.light_id = b.light_id;


-- Step 1.1: Set S9 to 70% — gives Cortex clear reason to increase
UPDATE LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES
SET brightness_pct = 70, saving_pct = 30
WHERE scenario_id = 9 AND light_id = 'ALL';

-- Step 2: Clear and inject test data
DELETE FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
WHERE timestamp >= DATEADD(day, -1, CURRENT_TIMESTAMP());

INSERT INTO LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
    (light_id, ldr_value, hour_of_day, timestamp)
SELECT
    'ALL',
    300 + (SEQ4() % 10),            -- LDR 300-310 (very bottom of moderate — very dark evening)
    18  + (SEQ4() % 3),             -- Hours 18-21 (evening peak)
    DATEADD(minute, -(SEQ4() * 20), CURRENT_TIMESTAMP())
FROM TABLE(GENERATOR(ROWCOUNT => 30));

-- Step 3: Check S9 BEFORE
SELECT 'BEFORE' AS when_, scenario_id, brightness_pct, saving_pct
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES
WHERE scenario_id = 9 AND light_id = 'ALL';

-- Step 4: Run SP
CALL LUMISENSE_DB.LUMISENSE_SCHEMA.SP_OPTIMIZE_SCENARIO_RULES();

-- 01c31af4-3202-748c-0014-aeda000220be
SELECT LAST_QUERY_ID();

SELECT *
FROM TABLE(RESULT_SCAN('01c31af4-3202-748c-0014-aeda000220be'));

-- Step 5: Check S9 AFTER
SELECT 'AFTER' AS when_, scenario_id, brightness_pct, saving_pct,
       TO_CHAR(timestamp, 'YYYY-MM-DD HH24:MI') AS last_updated
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES
WHERE scenario_id = 9 AND light_id = 'ALL';

SELECT *
FROM TABLE(RESULT_SCAN('01c31af5-3202-74cd-0014-aeda00021bce'));

-- -- Step 6: Check direction in log
-- SELECT TO_CHAR(timestamp,'YYYY-MM-DD HH24:MI') AS when_,
--        scenario_id, old_brightness, new_brightness,
--        CASE WHEN new_brightness > old_brightness THEN 'INCREASED'
--             WHEN new_brightness < old_brightness THEN 'REDUCED'
--             ELSE 'NO CHANGE' END AS direction,
--        reason
-- FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_OPTIMIZATION_LOG
-- ORDER BY timestamp DESC LIMIT 5;

-- SELECT *
-- FROM TABLE(RESULT_SCAN(''));


-- Step 7: Restore real data
DELETE FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
WHERE timestamp >= DATEADD(day, -1, CURRENT_TIMESTAMP());

INSERT INTO LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
SELECT * FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA_BACKUP;

-- EXPECTED: S9 INCREASED (80→85-90%), direction = INCREASED


-- ═══════════════════════════════════════════════════════════════
-- TEST 3 — SPARSE DATA → PRE-FILTER SHOULD SKIP
--
-- Scenario 6  : LDR 300-599, Hour 0-5, Midnight
-- Inject      : only 5 readings (below MIN_READINGS_REQUIRED=15)
-- Expected    : S6 PRE-FILTERED, brightness unchanged
-- ═══════════════════════════════════════════════════════════════

-- Step 1: Restore rules
-- UPDATE LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES r
-- SET brightness_pct = b.brightness_pct, saving_pct = b.saving_pct
-- FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES_BACKUP b
-- WHERE r.scenario_id = b.scenario_id AND r.light_id = b.light_id;

-- -- Step 2: Clear and inject only 5 readings
-- DELETE FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
-- WHERE timestamp >= DATEADD(day, -1, CURRENT_TIMESTAMP());

-- INSERT INTO LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
--     (light_id, ldr_value, hour_of_day, timestamp)
-- SELECT
--     'ALL',
--     350 + (SEQ4() % 100),           -- LDR 350-450
--     1   + (SEQ4() % 4),             -- Hours 1-4
--     DATEADD(minute, -(SEQ4() * 60), CURRENT_TIMESTAMP())
-- FROM TABLE(GENERATOR(ROWCOUNT => 5));   -- only 5!

-- -- Step 3: Confirm count
-- SELECT COUNT(*) AS "Readings (should be 5)"
-- FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
-- WHERE timestamp >= DATEADD(day, -1, CURRENT_TIMESTAMP());

-- -- Step 4: S6 BEFORE
-- SELECT 'BEFORE' AS when_, scenario_id, brightness_pct
-- FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES
-- WHERE scenario_id = 6 AND light_id = 'ALL';

-- -- Step 5: Run SP
-- CALL LUMISENSE_DB.LUMISENSE_SCHEMA.SP_OPTIMIZE_SCENARIO_RULES();

-- -- Step 6: S6 AFTER — must be unchanged
-- SELECT 'AFTER' AS when_, scenario_id, brightness_pct,
--        CASE WHEN brightness_pct = (
--            SELECT brightness_pct
--            FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES_BACKUP
--            WHERE scenario_id = 6 AND light_id = 'ALL'
--        ) THEN 'PASS — unchanged'
--          ELSE 'FAIL — should not have changed'
--        END AS result
-- FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES
-- WHERE scenario_id = 6 AND light_id = 'ALL';

-- -- Step 7: Restore real data
-- DELETE FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
-- WHERE timestamp >= DATEADD(day, -1, CURRENT_TIMESTAMP());
-- INSERT INTO LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
-- SELECT * FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA_BACKUP;

-- EXPECTED: PRE-FILTER shows "only 5 readings", S6 unchanged, result = PASS


-- ═══════════════════════════════════════════════════════════════
-- TEST 4 — MAX CHANGE CAP → BIG JUMP CAPPED AT 10%
--
-- Scenario 2  : LDR 0-299, Hour 6-8, Morning
-- Set S2 to 70%, inject pitch dark morning
-- Expected    : brightness = 80 (capped at 70+10)
-- ═══════════════════════════════════════════════════════════════

-- Step 1: Restore rules
UPDATE LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES r
SET brightness_pct = b.brightness_pct, saving_pct = b.saving_pct
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES_BACKUP b
WHERE r.scenario_id = b.scenario_id AND r.light_id = b.light_id;

-- Step 2: Set S2 to 70%
UPDATE LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES
SET brightness_pct = 70, saving_pct = 30
WHERE scenario_id = 2 AND light_id = 'ALL';

-- Step 3: Clear and inject pitch dark morning
DELETE FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
WHERE timestamp >= DATEADD(day, -1, CURRENT_TIMESTAMP());

INSERT INTO LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
    (light_id, ldr_value, hour_of_day, timestamp)
SELECT
    'ALL',
    10 + (SEQ4() % 50),             -- LDR 10-60 (pitch dark morning)
    6  + (SEQ4() % 2),              -- Hours 6-7
    DATEADD(minute, -(SEQ4() * 20), CURRENT_TIMESTAMP())
FROM TABLE(GENERATOR(ROWCOUNT => 30));

-- Step 4: S2 BEFORE
SELECT 'BEFORE' AS when_, scenario_id, brightness_pct
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES
WHERE scenario_id = 2 AND light_id = 'ALL';

-- Step 5: Run SP
CALL LUMISENSE_DB.LUMISENSE_SCHEMA.SP_OPTIMIZE_SCENARIO_RULES();

-- 01c31ae4-3202-74cd-0014-aeda00021a42
SELECT LAST_QUERY_ID();


SELECT *
FROM TABLE(RESULT_SCAN('01c31ae4-3202-74cd-0014-aeda00021a42'));

-- Step 6: Check cap held
SELECT 'AFTER' AS when_,
       scenario_id,
       brightness_pct AS after_brightness,
       70 AS was,
       brightness_pct - 70 AS actual_change,
       10 AS max_allowed,
       CASE WHEN brightness_pct - 70 <= 10
            THEN 'PASS — cap held'
            ELSE 'FAIL — cap exceeded'
       END AS result
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES
WHERE scenario_id = 2 AND light_id = 'ALL';


SELECT *
FROM TABLE(RESULT_SCAN('01c31ae8-3202-748c-0014-aeda0001dfd6'));


-- Step 7: Restore real data
DELETE FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
WHERE timestamp >= DATEADD(day, -1, CURRENT_TIMESTAMP());
INSERT INTO LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
SELECT * FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA_BACKUP;

-- EXPECTED: after_brightness = 80, actual_change = 10, result = PASS


-- ═══════════════════════════════════════════════════════════════
-- TEST 5 — FLOOR GUARDRAIL → CORTEX BLOCKED BELOW SAFETY MINIMUM
--
-- Scenario 4  : LDR 0-299, Hour 18-21, Evening (floor = 60%)
-- Set S4 to 65%, inject upper-dark-range evening data
-- Expected    : brightness stays >= 60%
-- ═══════════════════════════════════════════════════════════════

-- Step 1: Restore rules
UPDATE LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES r
SET brightness_pct = b.brightness_pct, saving_pct = b.saving_pct
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES_BACKUP b
WHERE r.scenario_id = b.scenario_id AND r.light_id = b.light_id;

-- Step 2: Set S4 to 65%
UPDATE LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES
SET brightness_pct = 65, saving_pct = 35
WHERE scenario_id = 4 AND light_id = 'ALL';

-- Step 3: Clear and inject upper dark range evening
DELETE FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
WHERE timestamp >= DATEADD(day, -1, CURRENT_TIMESTAMP());

INSERT INTO LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
    (light_id, ldr_value, hour_of_day, timestamp)
SELECT
    'ALL',
    250 + (SEQ4() % 40),            -- LDR 250-290 (upper dark range)
    18  + (SEQ4() % 3),             -- Hours 18-21
    DATEADD(minute, -(SEQ4() * 20), CURRENT_TIMESTAMP())
FROM TABLE(GENERATOR(ROWCOUNT => 30));

-- Step 4: S4 BEFORE
SELECT 'BEFORE' AS when_, scenario_id, brightness_pct
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES
WHERE scenario_id = 4 AND light_id = 'ALL';

-- Step 5: Run SP
CALL LUMISENSE_DB.LUMISENSE_SCHEMA.SP_OPTIMIZE_SCENARIO_RULES();

-- 01c31af7-3202-74cd-0014-aeda00021c2e
SELECT LAST_QUERY_ID();

SELECT *
FROM TABLE(RESULT_SCAN('01c31af7-3202-74cd-0014-aeda00021c2e'));

-- Step 6: Check floor held
SELECT 'AFTER' AS when_,
       scenario_id,
       brightness_pct AS after_brightness,
       60 AS floor_guardrail,
       CASE WHEN brightness_pct >= 60
            THEN 'PASS — floor held'
            ELSE 'FAIL — floor breached'
       END AS result
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES
WHERE scenario_id = 4 AND light_id = 'ALL';


-- 01c31af9-3202-748c-0014-aeda00022156
SELECT LAST_QUERY_ID();

SELECT *
FROM TABLE(RESULT_SCAN('01c31af9-3202-748c-0014-aeda00022156'));

-- Step 7: Restore real data
DELETE FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
WHERE timestamp >= DATEADD(day, -1, CURRENT_TIMESTAMP());
INSERT INTO LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
SELECT * FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA_BACKUP;

-- EXPECTED: after_brightness >= 60, result = PASS


-- ═══════════════════════════════════════════════════════════════
-- TEST 6 — NO DATA → PROCEDURE EXITS GRACEFULLY
--
-- Inject zero readings — SP should exit without calling Cortex
-- Expected    : "No scenarios have sufficient data" in SP log
--               Zero rule updates
-- ═══════════════════════════════════════════════════════════════

-- Step 1: Restore rules
UPDATE LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES r
SET brightness_pct = b.brightness_pct, saving_pct = b.saving_pct
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES_BACKUP b
WHERE r.scenario_id = b.scenario_id AND r.light_id = b.light_id;

-- Step 2: Clear all last 24h data — inject nothing
DELETE FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
WHERE timestamp >= DATEADD(day, -1, CURRENT_TIMESTAMP());

-- Step 3: Confirm zero readings
SELECT COUNT(*) AS "Readings (should be 0)"
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
WHERE timestamp >= DATEADD(day, -1, CURRENT_TIMESTAMP());

-- Step 4: Snapshot total brightness before
SELECT SUM(brightness_pct) AS total_before
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES
WHERE light_id = 'ALL';

-- Step 5: Run SP
CALL LUMISENSE_DB.LUMISENSE_SCHEMA.SP_OPTIMIZE_SCENARIO_RULES();

-- 01c31b01-3202-748c-0014-aeda000221e2
SELECT LAST_QUERY_ID();

SELECT *
FROM TABLE(RESULT_SCAN('01c31b01-3202-748c-0014-aeda000221e2'));

-- -- Step 6: Confirm no rules changed
-- SELECT SUM(brightness_pct) AS total_after,
--        CASE WHEN SUM(brightness_pct) = (
--            SELECT SUM(brightness_pct)
--            FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES_BACKUP
--            WHERE light_id = 'ALL'
--        ) THEN 'PASS — no rules changed'
--          ELSE 'FAIL — rules changed unexpectedly'
--        END AS result
-- FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES
-- WHERE light_id = 'ALL';

-- Step 7: Restore real data
DELETE FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
WHERE timestamp >= DATEADD(day, -1, CURRENT_TIMESTAMP());
INSERT INTO LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
SELECT * FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA_BACKUP;

-- EXPECTED: SP log shows "skipping Cortex call", result = PASS


-- ═══════════════════════════════════════════════════════════════
-- FINAL CLEANUP — Run after ALL tests done
-- ═══════════════════════════════════════════════════════════════

-- Restore rules to original
UPDATE LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES r
SET brightness_pct = b.brightness_pct,
    saving_pct     = b.saving_pct,
    timestamp      = b.timestamp
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES_BACKUP b
WHERE r.scenario_id = b.scenario_id AND r.light_id = b.light_id;

-- Restore real LDR data
DELETE FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
WHERE timestamp >= DATEADD(day, -1, CURRENT_TIMESTAMP());
INSERT INTO LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
SELECT * FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA_BACKUP;

-- Remove test optimization log entries
DELETE FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_OPTIMIZATION_LOG
WHERE timestamp >= DATEADD(hour, -3, CURRENT_TIMESTAMP());

-- Confirm rules restored
SELECT scenario_id, brightness_pct, saving_pct
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES
WHERE light_id = 'ALL'
ORDER BY scenario_id;

SELECT 'All done — real data and rules fully restored' AS status;