-- ═══════════════════════════════════════════════════════════════
-- LUMISENSE GRID — SELF IMPROVEMENT IN SNOWFLAKE
-- Uses : Snowflake Cortex + Stored Procedure + Task
-- Runs : Every day at 2AM UTC
--
-- AI AUTONOMY:
--   Cortex reads real sensor data + current rules + time patterns
--   It can increase OR decrease brightness based on evidence
--   Example: Dark road at peak evening → increase for safety
--   Example: Bright daylight all day  → decrease to save energy
--
-- TWO OPERATIONS PER CHANGED SCENARIO:
--   OPERATION 1 → UPDATE LDR_LED_SCENARIO_RULES
--                 brightness_pct, saving_pct, timestamp updated
--   OPERATION 2 → INSERT LDR_LED_OPTIMIZATION_LOG
--                 Full audit trail with timestamp of each change
--
-- SAFETY CHECKS (not restrictions — guardrails only):
--   1. Absolute brightness floor per scenario (safety for road users)
--   2. Absolute brightness ceiling per scenario (prevent overload)
--   3. Max change per run: 10% in either direction (gradual learning)
--   4. Minimum readings required (no changes on sparse data)
--   5. Saving % stays between 0 and 95
--   6. Cortex reason must be coherent with direction of change
-- ═══════════════════════════════════════════════════════════════


-- ───────────────────────────────────────────────────────────────
-- STEP 1: CREATE THE OPTIMIZATION STORED PROCEDURE
-- ───────────────────────────────────────────────────────────────
CREATE OR REPLACE PROCEDURE LUMISENSE_DB.LUMISENSE_SCHEMA.SP_OPTIMIZE_SCENARIO_RULES()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$

    // ── Safety guardrails per scenario ────────────────────────
    // These are hard floors and ceilings — not restrictions on direction
    // Floor = minimum brightness for road safety
    // Ceil  = maximum brightness (no reason to go above original design)
    const SAFETY = {
        //  id : { floor, ceil }
         1: { floor: 30, ceil: 60  },   // Dark  / Midnight
         2: { floor: 60, ceil: 100 },   // Dark  / Morning
         3: { floor: 40, ceil: 90  },   // Dark  / Daytime
         4: { floor: 60, ceil: 100 },   // Dark  / Evening
         5: { floor: 30, ceil: 70  },   // Dark  / Late Night
         6: { floor: 10, ceil: 40  },   // Mod   / Midnight
         7: { floor: 40, ceil: 80  },   // Mod   / Morning
         8: { floor: 15, ceil: 50  },   // Mod   / Daytime
         9: { floor: 50, ceil: 100 },   // Mod   / Evening
        10: { floor: 15, ceil: 50  },   // Mod   / Late Night
        11: { floor:  5, ceil: 25  },   // Bright/ Midnight
        12: { floor: 15, ceil: 45  },   // Bright/ Morning
        13: { floor:  5, ceil: 20  },   // Bright/ Daytime
        14: { floor: 30, ceil: 70  },   // Bright/ Evening
        15: { floor:  5, ceil: 30  },   // Bright/ Late Night
    };

    const MAX_CHANGE_PER_RUN    = 10;   // max % change in either direction per cycle
    const MIN_READINGS_REQUIRED = 15;  // minimum sensor readings to trust the data
    const MAX_SAVING_PCT        = 95;
    const MIN_SAVING_PCT        = 0;

    // ── Convert 24h to human-readable time so Cortex never confuses 6am vs 6pm ──
    function toAmPm(hour) {
        if (hour === 0)  return "12:00am (midnight)";
        if (hour < 12)   return hour + ":00am";
        if (hour === 12) return "12:00pm (noon)";
        return (hour - 12) + ":00pm";
    }

    function toTimeRange(hourMin, hourMax) {
        return toAmPm(hourMin) + " to " + toAmPm(hourMax);
    }

    var log = [];
    log.push("=== LumiSense Self-Optimization Started ===");
    log.push("Run time: " + new Date().toISOString());

    // ── 1. Gather last 24 hours of sensor data per scenario ──
    var statsResult = snowflake.execute({ sqlText: `
        SELECT
            r.scenario_id,
            r.brightness_pct                            AS current_brightness,
            r.saving_pct                                AS current_saving,
            r.ldr_min,
            r.ldr_max,
            r.hour_min,
            r.hour_max,
            r.energy_mode,
            COUNT(g.ldr_value)                          AS reading_count,
            ROUND(AVG(g.ldr_value),  0)                 AS avg_ldr,
            ROUND(MIN(g.ldr_value),  0)                 AS min_ldr,
            ROUND(MAX(g.ldr_value),  0)                 AS max_ldr,
            ROUND(STDDEV(g.ldr_value), 1)               AS stddev_ldr,
            COUNT(DISTINCT g.hour_of_day)               AS distinct_hours_seen
        FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES r
        LEFT JOIN LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA g
            ON  g.ldr_value    BETWEEN r.ldr_min  AND r.ldr_max
            AND g.hour_of_day  BETWEEN r.hour_min AND r.hour_max
            AND g.timestamp   >= DATEADD(day, -1, CURRENT_TIMESTAMP())
        WHERE r.light_id = 'ALL'
        GROUP BY
            r.scenario_id, r.brightness_pct, r.saving_pct,
            r.ldr_min, r.ldr_max, r.hour_min, r.hour_max, r.energy_mode
        ORDER BY r.scenario_id
    `});

    // Build rich stats text for Cortex — use human-readable time labels
    var statsText = "";
    statsText += "scenario_id | ldr_range  | time_of_day (24h)   | time_of_day (human)               | mode    | brightness | saving | readings | avg_ldr | min_ldr | max_ldr | stddev\n";
    statsText += "------------|------------|---------------------|-----------------------------------|---------|------------|--------|----------|---------|---------|---------|-------\n";

    var allScenarios      = [];   // all 15 — used for guardrail lookups
    var eligibleScenarios = [];   // only those with enough real data — sent to Cortex

    while (statsResult.next()) {
        var row = {
            scenario_id:        statsResult.getColumnValue("SCENARIO_ID"),
            current_brightness: statsResult.getColumnValue("CURRENT_BRIGHTNESS"),
            current_saving:     statsResult.getColumnValue("CURRENT_SAVING"),
            ldr_min:            statsResult.getColumnValue("LDR_MIN"),
            ldr_max:            statsResult.getColumnValue("LDR_MAX"),
            hour_min:           statsResult.getColumnValue("HOUR_MIN"),
            hour_max:           statsResult.getColumnValue("HOUR_MAX"),
            energy_mode:        statsResult.getColumnValue("ENERGY_MODE"),
            reading_count:      statsResult.getColumnValue("READING_COUNT")       || 0,
            avg_ldr:            statsResult.getColumnValue("AVG_LDR")             || 0,
            min_ldr:            statsResult.getColumnValue("MIN_LDR")             || 0,
            max_ldr:            statsResult.getColumnValue("MAX_LDR")             || 0,
            stddev_ldr:         statsResult.getColumnValue("STDDEV_LDR")          || 0,
            distinct_hours:     statsResult.getColumnValue("DISTINCT_HOURS_SEEN") || 0,
        };
        allScenarios.push(row);

        // ── PRE-FILTER: only eligible scenarios go to Cortex ──
        if (row.reading_count >= MIN_READINGS_REQUIRED) {
            eligibleScenarios.push(row);
        } else {
            log.push("[S" + row.scenario_id + "] PRE-FILTER — only " +
                     row.reading_count + " readings, excluded from Cortex prompt");
        }

    }

    // Build stats text ONLY from eligible scenarios
    for (var e = 0; e < eligibleScenarios.length; e++) {
        var row = eligibleScenarios[e];
        statsText +=
            row.scenario_id        + " | " +
            row.ldr_min + "-" + row.ldr_max                     + " | " +
            row.hour_min + ":00-" + row.hour_max + ":00 (24h)"  + " | " +
            toTimeRange(row.hour_min, row.hour_max)              + " | " +
            row.energy_mode        + " | " +
            row.current_brightness + "% | " +
            row.current_saving     + "% | " +
            row.reading_count      + " | " +
            row.avg_ldr            + " | " +
            row.min_ldr            + " | " +
            row.max_ldr            + " | " +
            row.stddev_ldr         + "\n";
    }

    log.push("Total scenarios     : " + allScenarios.length);
    log.push("Eligible for Cortex : " + eligibleScenarios.length + " (>= " + MIN_READINGS_REQUIRED + " readings)");

    // ── Exit early if nothing to analyze ─────────────────────
    if (eligibleScenarios.length === 0) {
        log.push("No scenarios have sufficient data — skipping Cortex call.");
        log.push("=== Optimization Complete — No Changes ===");
        return log.join("\n");
    }

    // ── 2. Call Snowflake Cortex ──────────────────────────────
    var prompt =
        "You are an expert energy optimization AI for a smart street lighting system.\n\n" +

        "Your job is to analyze real IoT sensor data and optimize brightness rules.\n" +
        "You can INCREASE or DECREASE brightness — your decision must be based entirely on the data.\n\n" +

        "LDR SENSOR CONTEXT:\n" +
        "- LDR 0-299    = Dark environment (night, no sunlight)\n" +
        "- LDR 300-599  = Moderate ambient light (dawn, dusk, cloudy)\n" +
        "- LDR 600-1023 = Bright environment (daylight, strong light)\n\n" +

        "TIME OF DAY - ALL HOURS USE 24-HOUR FORMAT. READ THIS CAREFULLY:\n" +
        "- hour 0  = 12:00am midnight\n" +
        "- hour 1  = 1:00am\n" +
        "- hour 2  = 2:00am\n" +
        "- hour 3  = 3:00am\n" +
        "- hour 4  = 4:00am\n" +
        "- hour 5  = 5:00am (early morning, before sunrise)\n" +
        "- hour 6  = 6:00am (6 in the MORNING, early morning, sunrise - this is NOT evening)\n" +
        "- hour 7  = 7:00am\n" +
        "- hour 8  = 8:00am\n" +
        "- hour 9  = 9:00am\n" +
        "- hour 10 = 10:00am\n" +
        "- hour 11 = 11:00am\n" +
        "- hour 12 = 12:00pm noon\n" +
        "- hour 13 = 1:00pm\n" +
        "- hour 14 = 2:00pm\n" +
        "- hour 15 = 3:00pm\n" +
        "- hour 16 = 4:00pm\n" +
        "- hour 17 = 5:00pm\n" +
        "- hour 18 = 6:00pm (6 in the EVENING - this is NOT 6am, this is after sunset)\n" +
        "- hour 19 = 7:00pm\n" +
        "- hour 20 = 8:00pm\n" +
        "- hour 21 = 9:00pm\n" +
        "- hour 22 = 10:00pm\n" +
        "- hour 23 = 11:00pm\n\n" +

        "CRITICAL RULE: hour 6 = 6:00am = morning. hour 18 = 6:00pm = evening. They are 12 hours apart and opposite times of day.\n\n" +

        "TIME WINDOWS IN THIS SYSTEM:\n" +
        "- hours 0 to 5   = 12:00am to 5:00am  = MIDNIGHT   (dark, minimal traffic)\n" +
        "- hours 6 to 8   = 6:00am  to 8:00am  = MORNING    (sunrise, rush hour, commuters)\n" +
        "- hours 9 to 17  = 9:00am  to 5:00pm  = DAYTIME    (full daylight, sun is up)\n" +
        "- hours 18 to 21 = 6:00pm  to 9:00pm  = EVENING    (after sunset, peak activity)\n" +
        "- hours 22 to 23 = 10:00pm to 11:00pm = LATE NIGHT (low traffic)\n\n" +

        "DECISION GUIDELINES:\n" +
        "- High avg_ldr + hours 9-17 (daytime)  = sunlight available, reduce artificial brightness\n" +
        "- Low avg_ldr  + hours 6-8  (morning)  = dark morning, increase brightness for commuters\n" +
        "- Low avg_ldr  + hours 18-21 (evening) = dark evening, increase brightness for safety\n" +
        "- High stddev = inconsistent conditions, make smaller adjustment\n" +
        "- The data table includes time_of_day (human) column showing exact real-world time. Use it.\n\n" +

        "IMPORTANT: You must ONLY suggest changes for these scenario IDs: [" + eligibleScenarios.map(function(r) { return r.scenario_id; }).join(", ") + "]\n" +
        "These are the ONLY scenarios with real sensor data. Do NOT suggest any other scenario ID.\n\n" +

        "REAL SENSOR DATA FROM LAST 24 HOURS (only eligible scenarios shown):\n" +
        statsText + "\n" +

        "Respond ONLY in this exact JSON format, no extra text:\n" +
        "[\n" +
        "  {\n" +
        "    \"scenario_id\": 2,\n" +
        "    \"new_brightness\": 80,\n" +
        "    \"reason\": \"hour range 6:00am-8:00am morning rush, avg LDR 35 is very dark, increase from 70% to 80% for commuter safety\"\n" +
        "  }\n" +
        "]\n\n" +
        "Rules:\n" +
        "- Use the time_of_day (human) column to understand the real-world time of each scenario\n" +
        "- Only suggest changes clearly justified by the sensor data\n" +
        "- If no change is needed, return: []\n" +
        "- Do NOT include markdown, backticks, or any text outside the JSON";

    var cortexResult = snowflake.execute({
        sqlText: "SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-7b', ?) AS response",
        binds:   [prompt]
    });

    cortexResult.next();
    var rawResponse = cortexResult.getColumnValue("RESPONSE");
    log.push("Cortex responded successfully");

    // ── 3. Parse JSON from Cortex response ───────────────────
    var suggestions = [];
    try {
        var jsonMatch = rawResponse.match(/\[[\s\S]*\]/);
        if (jsonMatch) {
            suggestions = JSON.parse(jsonMatch[0]);
        } else {
            log.push("No JSON array found in Cortex response");
            log.push("Raw response: " + rawResponse.substring(0, 300));
            return log.join("\n");
        }
    } catch(e) {
        log.push("Failed to parse Cortex JSON: " + e.message);
        log.push("Raw response: " + rawResponse.substring(0, 300));
        return log.join("\n");
    }

    log.push("Cortex suggested " + suggestions.length + " scenario changes");

    if (suggestions.length === 0) {
        log.push("No changes needed this cycle — rules are already well optimized.");
        return log.join("\n");
    }

    // ── 4. Safety guardrails + apply changes ─────────────────
    var updated = 0;
    var skipped = 0;

    for (var i = 0; i < suggestions.length; i++) {
        var s            = suggestions[i];
        var scenarioId   = parseInt(s.scenario_id);
        var newBrightness = parseInt(s.new_brightness);
        var reason        = (s.reason || "").toString().trim();

        // Find the matching scenario from allScenarios (full list for guardrails)
        var scenario = null;
        for (var j = 0; j < allScenarios.length; j++) {
            if (allScenarios[j].scenario_id === scenarioId) {
                scenario = allScenarios[j];
                break;
            }
        }

        // ── Hallucination check: reject scenario IDs not in eligible list ──
        var isEligible = false;
        for (var k = 0; k < eligibleScenarios.length; k++) {
            if (eligibleScenarios[k].scenario_id === scenarioId) {
                isEligible = true;
                break;
            }
        }
        if (!isEligible) {
            log.push("[S" + scenarioId + "] REJECTED — Cortex hallucinated this scenario (no real data)");
            skipped++;
            continue;
        }

        if (!scenario) {
            log.push("[S" + scenarioId + "] SKIP — scenario not found");
            skipped++;
            continue;
        }

        var oldBrightness = scenario.current_brightness;
        var guard         = SAFETY[scenarioId];

        if (!guard) {
            log.push("[S" + scenarioId + "] SKIP — no safety guardrail defined");
            skipped++;
            continue;
        }

        // ── Guardrail 1: Minimum readings ─────────────────────
        if (scenario.reading_count < MIN_READINGS_REQUIRED) {
            log.push("[S" + scenarioId + "] SKIP — only " + scenario.reading_count +
                     " readings (need " + MIN_READINGS_REQUIRED + ")");
            skipped++;
            continue;
        }

        // ── Guardrail 2: No-op check ───────────────────────────
        if (newBrightness === oldBrightness) {
            log.push("[S" + scenarioId + "] SKIP — no change (same value " + oldBrightness + "%)");
            skipped++;
            continue;
        }

        // ── Guardrail 3: Max change per run (either direction) ─
        var change = newBrightness - oldBrightness;
        if (Math.abs(change) > MAX_CHANGE_PER_RUN) {
            // Cap the change, preserve direction
            newBrightness = oldBrightness + (change > 0 ? MAX_CHANGE_PER_RUN : -MAX_CHANGE_PER_RUN);
            log.push("[S" + scenarioId + "] CAPPED — change exceeded " + MAX_CHANGE_PER_RUN +
                     "%, adjusted to " + newBrightness + "%");
        }

        // ── Guardrail 4: Floor check (safety minimum) ─────────
        if (newBrightness < guard.floor) {
            log.push("[S" + scenarioId + "] SKIP — " + newBrightness +
                     "% is below safety floor " + guard.floor + "%");
            skipped++;
            continue;
        }

        // ── Guardrail 5: Ceiling check (no over-illumination) ─
        if (newBrightness > guard.ceil) {
            log.push("[S" + scenarioId + "] SKIP — " + newBrightness +
                     "% exceeds safety ceiling " + guard.ceil + "%");
            skipped++;
            continue;
        }

        // ── Guardrail 6: Reason coherence check ───────────────
        var lowerReason  = reason.toLowerCase();
        var isIncreasing = newBrightness > oldBrightness;
        var isDecreasing = newBrightness < oldBrightness;

        // Reason says reduce but values show increase (or vice versa)
        var reasonSaysReduce  = lowerReason.indexOf("reduc") !== -1 || lowerReason.indexOf("decreas") !== -1 || lowerReason.indexOf("lower") !== -1;
        var reasonSaysIncrease = lowerReason.indexOf("increas") !== -1 || lowerReason.indexOf("higher") !== -1 || lowerReason.indexOf("raise") !== -1;

        if (isIncreasing && reasonSaysReduce && !reasonSaysIncrease) {
            log.push("[S" + scenarioId + "] SKIP — reason says reduce but value is increasing");
            skipped++;
            continue;
        }
        if (isDecreasing && reasonSaysIncrease && !reasonSaysReduce) {
            log.push("[S" + scenarioId + "] SKIP — reason says increase but value is decreasing");
            skipped++;
            continue;
        }

        // ── Calculate new saving % ─────────────────────────────
        // More brightness = less saving | Less brightness = more saving
        // saving is always 100 - brightness (same formula used in the rules table)
        var newSaving = 100 - newBrightness;

        // ── OPERATION 1: UPDATE LDR_LED_SCENARIO_RULES ────────
        snowflake.execute({
            sqlText: `
                UPDATE LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES
                SET brightness_pct = ?,
                    saving_pct     = ?,
                    timestamp      = CURRENT_TIMESTAMP()
                WHERE scenario_id  = ?
                  AND light_id     = 'ALL'
            `,
            binds: [newBrightness, newSaving, scenarioId]
        });

        // ── OPERATION 2: INSERT LDR_LED_OPTIMIZATION_LOG ──────
        snowflake.execute({
            sqlText: `
                INSERT INTO LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_OPTIMIZATION_LOG
                    (light_id, scenario_id,
                     old_brightness, new_brightness,
                     old_saving, new_saving,
                     reason)
                VALUES ('ALL', ?, ?, ?, ?, ?, ?)
            `,
            binds: [
                scenarioId,
                oldBrightness, newBrightness,
                scenario.current_saving, newSaving,
                reason
            ]
        });

        var direction = isIncreasing ? "↑ INCREASED" : "↓ REDUCED";
        log.push("[S" + scenarioId + "] " + direction + " " +
                 oldBrightness + "% → " + newBrightness + "%" +
                 " | Saving: " + scenario.current_saving + "% → " + newSaving + "%" +
                 " | Reason: " + reason);
        updated++;
    }

    log.push("\n=== Optimization Complete ===");
    log.push("Updated  : " + updated  + " scenarios");
    log.push("Skipped  : " + skipped  + " scenarios");
    log.push("Run time : " + new Date().toISOString());

    return log.join("\n");
$$;


-- ───────────────────────────────────────────────────────────────
-- STEP 2: TEST MANUALLY BEFORE SCHEDULING
-- Run this first, check the output carefully before activating task
-- ───────────────────────────────────────────────────────────────
CALL LUMISENSE_DB.LUMISENSE_SCHEMA.SP_OPTIMIZE_SCENARIO_RULES();


-- ───────────────────────────────────────────────────────────────
-- STEP 3: CREATE TASK — RUNS EVERY DAY AT 2AM IST
-- ───────────────────────────────────────────────────────────────
CREATE OR REPLACE TASK LUMISENSE_DB.LUMISENSE_SCHEMA.TASK_OPTIMIZE_SCENARIO_RULES
    WAREHOUSE = COMPUTE_WH
    SCHEDULE  = 'USING CRON 30 20 * * * UTC'
AS
    CALL LUMISENSE_DB.LUMISENSE_SCHEMA.SP_OPTIMIZE_SCENARIO_RULES();


-- ───────────────────────────────────────────────────────────────
-- STEP 4: ACTIVATE THE TASK
-- ───────────────────────────────────────────────────────────────
ALTER TASK LUMISENSE_DB.LUMISENSE_SCHEMA.TASK_OPTIMIZE_SCENARIO_RULES RESUME;


-- ───────────────────────────────────────────────────────────────
-- MONITORING QUERIES
-- ───────────────────────────────────────────────────────────────

-- Check task run history
SELECT
    name,
    state,
    scheduled_time,
    completed_time,
    error_message
FROM TABLE(LUMISENSE_DB.information_schema.task_history())
WHERE name = 'TASK_OPTIMIZE_SCENARIO_RULES'
ORDER BY scheduled_time DESC
LIMIT 10;


-- Full optimization history with direction
SELECT
    TO_CHAR(timestamp, 'YYYY-MM-DD HH24:MI')    AS "When",
    scenario_id                                  AS "Scenario",
    old_brightness || '%'                        AS "Was",
    new_brightness || '%'                        AS "Now",
    CASE
        WHEN new_brightness > old_brightness THEN '↑ Increased'
        WHEN new_brightness < old_brightness THEN '↓ Reduced'
        ELSE '— No Change'
    END                                          AS "Direction",
    ABS(old_brightness - new_brightness) || '%'  AS "Changed By",
    old_saving || '%'                            AS "Old Saving",
    new_saving || '%'                            AS "New Saving",
    reason                                       AS "Cortex Reason"
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_OPTIMIZATION_LOG
ORDER BY timestamp DESC
LIMIT 30;


-- Current state of all rules after optimization
SELECT
    scenario_id                                     AS "Scenario",
    ldr_min || '-' || ldr_max                       AS "LDR Range",
    hour_min || '-' || hour_max                     AS "Hour Range",
    energy_mode                                     AS "Mode",
    brightness_pct || '%'                           AS "Brightness",
    saving_pct || '%'                               AS "Saving",
    TO_CHAR(timestamp, 'YYYY-MM-DD HH24:MI')        AS "Last Updated By AI"
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES
WHERE light_id = 'ALL'
ORDER BY scenario_id;


-- Pause task if needed
-- ALTER TASK LUMISENSE_DB.LUMISENSE_SCHEMA.TASK_OPTIMIZE_SCENARIO_RULES SUSPEND;