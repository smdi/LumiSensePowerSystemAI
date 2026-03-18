



CREATE OR REPLACE PROCEDURE LUMISENSE_DB.LUMISENSE_SCHEMA.SP_DETECT_SENSOR_FAULTS()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
var log = [];
log.push("=== Sensor Fault Detection Started ===");

// ─────────────────────────────────────────────
// STEP 1: Fetch stats
// ─────────────────────────────────────────────
var rs = snowflake.execute({
    sqlText: `
        SELECT
            light_id,
            COUNT(*) AS reading_count,

            COALESCE(ROUND(AVG(ldr_value),0),0) AS avg_ldr,
            COALESCE(MIN(ldr_value),0) AS min_ldr,
            COALESCE(MAX(ldr_value),0) AS max_ldr,
            COALESCE(ROUND(STDDEV(ldr_value),1),0) AS stddev,

            COALESCE(DATEDIFF('minute', MAX(timestamp), CURRENT_TIMESTAMP()),9999) AS mins,

            COALESCE(ROUND(AVG(CASE WHEN hour_of_day BETWEEN 9 AND 17 THEN ldr_value END),0),-1) AS day,
            COALESCE(ROUND(AVG(CASE WHEN hour_of_day BETWEEN 0 AND 5 THEN ldr_value END),0),-1) AS night

        FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
        GROUP BY light_id
        ORDER BY light_id
    `
});

var sensors = [];
while (rs.next()) {
    sensors.push({
        light_id: rs.getColumnValue("LIGHT_ID"),
        reading_count: rs.getColumnValue("READING_COUNT"),
        avg_ldr: rs.getColumnValue("AVG_LDR"),
        min_ldr: rs.getColumnValue("MIN_LDR"),
        max_ldr: rs.getColumnValue("MAX_LDR"),
        stddev: rs.getColumnValue("STDDEV"),
        mins: rs.getColumnValue("MINS"),
        day: rs.getColumnValue("DAY"),
        night: rs.getColumnValue("NIGHT")
    });
}

log.push("Sensors found: " + sensors.length);

// ─────────────────────────────────────────────
// STEP 2: Rule-based detection (FINAL LOGIC)
// ─────────────────────────────────────────────
for (var i = 0; i < sensors.length; i++) {

    var s = sensors[i];

    var fault = "HEALTHY";
    var severity = "OK";
    var reason = "Working normally";
    var action = "No action needed";

    // 🔴 HARD FAILS
    if (s.min_ldr === 0 && s.max_ldr === 0) {
        fault = "DEAD";
        severity = "CRITICAL";
        reason = "Sensor always 0 → disconnected";
        action = "Check wiring or replace LDR";
    }
    else if (s.min_ldr === 1023 && s.max_ldr === 1023) {
        fault = "SHORT_CIRCUIT";
        severity = "CRITICAL";
        reason = "Sensor stuck at max";
        action = "Check for short circuit (VCC to signal)";
    }

    // 🔴 DATA RECENCY
    else if (s.mins > 60) {
        fault = "SILENT";
        severity = "CRITICAL";
        reason = "No recent data";
        action = "Check power, Arduino, or connection";
    }

    // 🟡 DATA QUALITY
    else if (s.reading_count < 3) {
        fault = "INSUFFICIENT_DATA";
        severity = "WARNING";
        reason = "Too few readings";
        action = "Collect more data";
    }

    // 🟡 BEHAVIOR
    else if (s.stddev < 10) {
        fault = "FROZEN";
        severity = "WARNING";
        reason = "No variation in readings";
        action = "Sensor stuck, check placement";
    }
    else if (s.stddev > 400) {
        fault = "NOISY";
        severity = "WARNING";
        reason = "Too much fluctuation";
        action = "Check loose wiring or interference";
    }
    else if (s.day !== -1 && s.day < 300) {
        fault = "WRONG_PATTERN";
        severity = "WARNING";
        reason = "Low daylight readings";
        action = "Check sensor exposure to light";
    }
    else if (s.night !== -1 && s.night > 500) {
        fault = "WRONG_PATTERN";
        severity = "WARNING";
        reason = "High night readings";
        action = "Check for artificial light interference";
    }

    // ─────────────────────────────
    // OUTPUT
    // ─────────────────────────────
    log.push("🔹 SENSOR: " + s.light_id);
    log.push("Status: " + fault + " (" + severity + ")");
    log.push("Reason: " + reason);
    log.push("Action: " + action);
    log.push("----------------------------------");

    var action = "No action needed";
    // Insert only faulty sensors
    if (fault !== "HEALTHY") {
        snowflake.execute({
            sqlText: `
                INSERT INTO LUMISENSE_DB.LUMISENSE_SCHEMA.SENSOR_HEALTH_LOG
                (light_id, fault_type, severity, reason, action)
                VALUES (?, ?, ?, ?, ?)
            `,
            binds: [s.light_id, fault, severity, reason, action]
        });

        var prompt = `
        You are an IoT sensor expert.
        
        Sensor: ${s.light_id}
        Fault: ${fault}
        Reason: ${reason}
        
        Give ONLY:
        <short explanation> | <practical fix>
        
        Rules:
        - one line only
        - no extra text
        - no formatting
        `;
        
            try {
                var cortexRes = snowflake.execute({
                    sqlText: `
                        SELECT SNOWFLAKE.CORTEX.COMPLETE(
                            'mistral-large',
                            ?
                        )
                    `,
                    binds: [prompt]
                });
        
                cortexRes.next();
                action = cortexRes.getColumnValue(1);
        
            } catch (err) {
                action = "AI failed | Check manually";
            }
    
    }
}

log.push("=== Completed ===");
return log.join("\n");
$$;

-- Schedule it to run every hour
CREATE OR REPLACE TASK LUMISENSE_DB.LUMISENSE_SCHEMA.TASK_SENSOR_HEALTH
    WAREHOUSE = COMPUTE_WH
    SCHEDULE  = 'USING CRON 30 20,8 * * * UTC'   -- every twelve hours
AS
    CALL LUMISENSE_DB.LUMISENSE_SCHEMA.SP_DETECT_SENSOR_FAULTS();

-- 
ALTER TASK LUMISENSE_DB.LUMISENSE_SCHEMA.TASK_SENSOR_HEALTH RESUME;


-- Check task run history
SELECT
    name,
    state,
    scheduled_time,
    completed_time,
    error_message
FROM TABLE(LUMISENSE_DB.information_schema.task_history())
WHERE name = 'TASK_SENSOR_HEALTH'
ORDER BY scheduled_time DESC
LIMIT 10;


-- Dashboard tile — live sensor health
-- SELECT
--     light_id                                    AS "Light",
--     fault_type                                  AS "Fault",
--     severity                                    AS "Severity",
--     reason                                      AS "Reason",
--     action                                      AS "Action Required",
--     TO_CHAR(timestamp, 'YYYY-MM-DD HH24:MI')    AS "Detected At"
-- FROM LUMISENSE_DB.LUMISENSE_SCHEMA.SENSOR_HEALTH_LOG
-- WHERE timestamp >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
-- ORDER BY
--     CASE severity
--         WHEN 'CRITICAL' THEN 1
--         WHEN 'WARNING'  THEN 2
--         ELSE 3
--     END,
--     timestamp DESC;







-- Test scenarios 1
-- Healthy Sensor (should return OK / empty JSON)
DELETE FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
WHERE LIGHT_ID IN ('L1', 'L2', 'L3', 'L4', 'L5', 'L6', 'L7', 'TEST', 'ALL')
;


INSERT INTO LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
(light_id, ldr_value, hour_of_day, timestamp)
VALUES
('L1', 720, 10, CURRENT_TIMESTAMP()),
('L1', 690, 11, CURRENT_TIMESTAMP()),
('L1', 650, 12, CURRENT_TIMESTAMP()),
('L1', 700, 13, CURRENT_TIMESTAMP()),
('L1', 180, 1, CURRENT_TIMESTAMP()),
('L1', 120, 2, CURRENT_TIMESTAMP()),
('L1', 90, 3, CURRENT_TIMESTAMP()),
('L1', 110, 4, CURRENT_TIMESTAMP());



-- Test scenario 2
-- DEAD SENSOR (min=0 max=0)

INSERT INTO LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
(light_id, ldr_value, hour_of_day, timestamp)
VALUES
('L2',0,10,CURRENT_TIMESTAMP()),
('L2',0,11,CURRENT_TIMESTAMP()),
('L2',0,12,CURRENT_TIMESTAMP()),
('L2',0,1,CURRENT_TIMESTAMP()),
('L2',0,2,CURRENT_TIMESTAMP());


-- Test scenario 3
-- SHORT CIRCUIT SENSOR (always 1023)

INSERT INTO LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
(light_id, ldr_value, hour_of_day, timestamp)
VALUES
('L3',1023,10,CURRENT_TIMESTAMP()),
('L3',1023,11,CURRENT_TIMESTAMP()),
('L3',1023,12,CURRENT_TIMESTAMP()),
('L3',1023,1,CURRENT_TIMESTAMP());


-- Test scenario 4
-- FROZEN SENSOR (stddev < 10)

INSERT INTO LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
(light_id, ldr_value, hour_of_day, timestamp)
VALUES
('L4',450,10,CURRENT_TIMESTAMP()),
('L4',451,11,CURRENT_TIMESTAMP()),
('L4',450,12,CURRENT_TIMESTAMP()),
('L4',451,13,CURRENT_TIMESTAMP()),
('L4',450,14,CURRENT_TIMESTAMP());


-- Test scenario 5
-- NOISY SENSOR (stddev > 400)

INSERT INTO LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
(light_id, ldr_value, hour_of_day, timestamp)
VALUES
('L5',50,10,CURRENT_TIMESTAMP()),
('L5',900,11,CURRENT_TIMESTAMP()),
('L5',120,12,CURRENT_TIMESTAMP()),
('L5',950,13,CURRENT_TIMESTAMP()),
('L5',80,14,CURRENT_TIMESTAMP());


-- Test scenario 6
-- WRONG PATTERN (bright at night)

INSERT INTO LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
(light_id, ldr_value, hour_of_day, timestamp)
VALUES
('L6',650,1,CURRENT_TIMESTAMP()),
('L6',700,2,CURRENT_TIMESTAMP()),
('L6',680,3,CURRENT_TIMESTAMP()),
('L6',720,4,CURRENT_TIMESTAMP());


-- Test scenario 7
-- SILENT SENSOR (no recent readings)

INSERT INTO LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
(light_id, ldr_value, hour_of_day, timestamp)
VALUES
('L7',700,12,DATEADD(hour,-5,CURRENT_TIMESTAMP())),
('L7',680,13,DATEADD(hour,-5,CURRENT_TIMESTAMP()));





-- Run the procedure

CALL LUMISENSE_DB.LUMISENSE_SCHEMA.SP_DETECT_SENSOR_FAULTS();


-- 01c317a7-3202-748c-0014-aeda0001d7c2
SELECT LAST_QUERY_ID();


SELECT *
FROM TABLE(RESULT_SCAN('01c317a7-3202-748c-0014-aeda0001d7c2'));


SELECT *
FROM LUMISENSE_DB.LUMISENSE_SCHEMA.SENSOR_HEALTH_LOG
ORDER BY timestamp DESC;

-- DELETE FROM LUMISENSE_DB.LUMISENSE_SCHEMA.SENSOR_HEALTH_LOG;




-- DASHBOARD DATA
-- INSERT INTO LUMISENSE_DB.LUMISENSE_SCHEMA.SENSOR_HEALTH_LOG
-- (light_id, fault_type, severity, reason, action, timestamp)

-- WITH faults AS (
--     SELECT * FROM VALUES
--         ('DEAD', 'CRITICAL'),
--         ('FROZEN', 'WARNING'),
--         ('INSUFFICIENT_DATA', 'WARNING'),
--         ('NOISY', 'WARNING'),
--         ('SHORT_CIRCUIT', 'CRITICAL'),
--         ('SILENT', 'CRITICAL'),
--         ('WRONG_PATTERN', 'WARNING')
--     AS t(fault_type, severity)
-- ),

-- lights AS (
--     SELECT SEQ4() + 1 AS light_id
--     FROM TABLE(GENERATOR(ROWCOUNT => 50))  -- 50 lights
-- ),

-- time_series AS (
--     SELECT DATEADD(hour, -SEQ4(), CURRENT_TIMESTAMP()) AS ts
--     FROM TABLE(GENERATOR(ROWCOUNT => 48))  -- last 48 hours
-- ),

-- mock_data AS (
--     SELECT
--         l.light_id,
--         f.fault_type,
--         f.severity,
--         CASE 
--             WHEN f.fault_type = 'DEAD' THEN 'No signal detected'
--             WHEN f.fault_type = 'FROZEN' THEN 'Sensor stuck'
--             WHEN f.fault_type = 'NOISY' THEN 'High variance detected'
--             WHEN f.fault_type = 'SHORT_CIRCUIT' THEN 'Electrical fault'
--             WHEN f.fault_type = 'SILENT' THEN 'No transmission'
--             ELSE 'Irregular pattern'
--         END AS reason,
--         CASE 
--             WHEN f.severity = 'CRITICAL' THEN 'Immediate inspection required'
--             ELSE 'Monitor closely'
--         END AS action,
--         t.ts AS timestamp,
--         UNIFORM(0, 5, RANDOM()) AS fault_count
--     FROM lights l
--     CROSS JOIN faults f
--     CROSS JOIN time_series t
-- )

-- SELECT
--     light_id,
--     fault_type,
--     severity,
--     reason,
--     action,
--     timestamp
-- FROM mock_data
-- WHERE fault_count > 0;