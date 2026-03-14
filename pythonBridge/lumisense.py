import snowflake.connector
import serial
import time
import os
import csv
from datetime import datetime
from dotenv import load_dotenv


# ─── LOAD .env ────────────────────────────────────────────────
load_dotenv()


# ─── CONFIG ───────────────────────────────────────
SERIAL_PORT = 'COM5'
BAUD_RATE = 9600

DEBUG_MODE  = True
BATCH_SIZE  = 3 if DEBUG_MODE else 10

CACHE_FILE         = 'rules_cache.csv'   # stored in same folder as script
CACHE_REFRESH_HOUR = 2                   # 2 AM
CACHE_REFRESH_MIN  = 10                  # 2:10 AM — 10 min after task runs

# ─── CONFIG FROM .env ─────────────────────────────────────────
SNOWFLAKE_CONFIG = {
    'user':      os.getenv('SNOWFLAKE_USER'),
    'password':  os.getenv('SNOWFLAKE_PASSWORD'),
    'account':   os.getenv('SNOWFLAKE_ACCOUNT'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'database':  os.getenv('SNOWFLAKE_DATABASE'),
    'schema':    os.getenv('SNOWFLAKE_SCHEMA'),
}

# ─── VALIDATE CONFIG ──────────────────────────────────────────
def validate_config():
    required = ['user', 'password', 'account', 'warehouse', 'database', 'schema']
    missing  = [k for k in required if not SNOWFLAKE_CONFIG.get(k)]
    if missing:
        raise ValueError(
            f"Missing in .env: {', '.join(['SNOWFLAKE_' + k.upper() for k in missing])}"
        )
 
 
# ═══════════════════════════════════════════════════════════════
# CACHE FUNCTIONS
# ═══════════════════════════════════════════════════════════════
 
def fetch_rules_from_snowflake(cursor):
    """Fetch latest rules from Snowflake and return as list of dicts"""
    print("🔄 Fetching rules from Snowflake...")
    cursor.execute("""
        SELECT scenario_id, light_id,
               ldr_min, ldr_max,
               hour_min, hour_max,
               brightness_pct, energy_mode, saving_pct,
               timestamp
        FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES
        WHERE light_id = 'ALL'
        ORDER BY scenario_id
    """)
    rows  = cursor.fetchall()
    rules = []
    for row in rows:
        rules.append({
            'scenario_id':    row[0],
            'light_id':       row[1],   # always 'ALL' — stored in CSV, not used for matching
            'ldr_min':        row[2],
            'ldr_max':        row[3],
            'hour_min':       row[4],
            'hour_max':       row[5],
            'brightness_pct': row[6],
            'energy_mode':    row[7],
            'saving_pct':     row[8],
            'timestamp':      str(row[9]),
        })
    print(f"✅ {len(rules)} rules fetched from Snowflake")
    return rules
 
 
def save_rules_to_csv(rules):
    """Save rules to CSV file with refresh timestamp"""
    with open(CACHE_FILE, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'scenario_id', 'light_id',
            'ldr_min', 'ldr_max',
            'hour_min', 'hour_max',
            'brightness_pct', 'energy_mode', 'saving_pct',
            'timestamp', 'cached_at'
        ])
        writer.writeheader()
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for rule in rules:
            row = dict(rule)
            row['cached_at'] = now
            writer.writerow(row)
    print(f"💾 Rules saved to {CACHE_FILE}")
 
 
def load_rules_from_csv():
    """Load rules from CSV file into memory. Returns empty list if CSV is empty."""
    rules = []
    with open(CACHE_FILE, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rules.append({
                'scenario_id':    int(row['scenario_id']),
                'light_id':       row['light_id'],   # stored only — not used for matching
                'ldr_min':        int(row['ldr_min']),
                'ldr_max':        int(row['ldr_max']),
                'hour_min':       int(row['hour_min']),
                'hour_max':       int(row['hour_max']),
                'brightness_pct': int(row['brightness_pct']),
                'energy_mode':    row['energy_mode'],
                'saving_pct':     int(row['saving_pct']),
                'timestamp':      row['timestamp'],
                'cached_at':      row['cached_at'],
            })
    if rules:
        print(f"📂 {len(rules)} rules loaded from cache "
              f"(cached at: {rules[0]['cached_at']})")
    else:
        print("⚠️  Cache file exists but is empty")
    return rules
 
 
def initialize_cache(cursor):
    """
    On startup:
    - If CSV exists and has data → load from CSV (no Snowflake query)
    - If CSV missing or empty    → fetch from Snowflake and create CSV
    """
    if os.path.exists(CACHE_FILE):
        print(f"📂 Cache file found — loading from {CACHE_FILE}")
        rules = load_rules_from_csv()
        if rules:
            return rules
        else:
            print("📂 Cache empty — fetching from Snowflake...")
            rules = fetch_rules_from_snowflake(cursor)
            save_rules_to_csv(rules)
            return rules
    else:
        print("📂 No cache file — fetching from Snowflake to create cache...")
        rules = fetch_rules_from_snowflake(cursor)
        save_rules_to_csv(rules)
        return rules
 
 
def refresh_cache_if_needed(cursor, rules_cache):
    """
    At 2:10 AM every day — refresh cache from Snowflake
    Snowflake task runs at 2:00 AM, we wait 10 min to ensure it completes
    """
    now = datetime.now()
    if now.hour == CACHE_REFRESH_HOUR and now.minute == CACHE_REFRESH_MIN:
        print("\n⏰ 2:10 AM — refreshing rules cache from Snowflake...")
        rules = fetch_rules_from_snowflake(cursor)
        save_rules_to_csv(rules)
        print("✅ Cache refreshed with latest AI-optimized rules!\n")
        return rules
    return rules_cache
 
 
# ═══════════════════════════════════════════════════════════════
# ARDUINO IDENTITY
# ═══════════════════════════════════════════════════════════════
 
def wait_for_light_id(ser):
    """
    Wait for Arduino to send its identity on startup
    Arduino sends "ID:L1" as the first message
    Keeps reading until ID is received
    """
    print("⏳ Waiting for Arduino to identify itself...")
    while True:
        raw = ser.readline().decode().strip()
        if raw.startswith("ID:"):
            light_id = raw.split(":")[1]
            print(f"🔌 Arduino identified as: {light_id}")
            return light_id
        else:
            # Could be DEBUG:ON or other startup messages
            if raw:
                print(f"ℹ️  Arduino: {raw}")
 
 
# ═══════════════════════════════════════════════════════════════
# CORE BRIDGE FUNCTIONS
# ═══════════════════════════════════════════════════════════════
 
def write_LDR_GRID_DATA(cursor, conn, light_id, ldr_value, hour_of_day):
    """Write LDR reading to Snowflake"""
    cursor.execute("""
        INSERT INTO LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
            (light_id, ldr_value, hour_of_day)
        VALUES (%s, %s, %s)
    """, (light_id, ldr_value, hour_of_day))
    conn.commit()
    print(f"✅ LDR saved → Light: {light_id} | "
          f"LDR: {ldr_value} | Hour: {hour_of_day}")
 
 
def read_LDR_LED_SCENARIO_RULES(rules_cache, ldr_value, hour_of_day):
    """
    Read brightness decision from CSV cache — zero Snowflake queries
    Matches LDR + hour against in-memory rules loaded from CSV
    """
    for rule in rules_cache:
        if (rule['ldr_min'] <= ldr_value <= rule['ldr_max'] and
                rule['hour_min'] <= hour_of_day <= rule['hour_max']):
            print(f"📊 Cache hit →"
                  f" Scenario: {rule['scenario_id']}"
                  f" | LDR: {ldr_value} (range {rule['ldr_min']}-{rule['ldr_max']})"
                  f" | Hour: {hour_of_day} (range {rule['hour_min']}-{rule['hour_max']})"
                  f" | Brightness: {rule['brightness_pct']}%"
                  f" | Mode: {rule['energy_mode']}"
                  f" | Saving: {rule['saving_pct']}%")
            return {
                'brightness': rule['brightness_pct'],
                'mode':       rule['energy_mode'],
                'saving':     rule['saving_pct'],
            }
 
    print("⚠️  No rule matched in cache — using default 50%")
    return {'brightness': 50, 'mode': 'NORMAL', 'saving': 50}
 
 
def send_to_arduino(ser, brightness):
    """Send brightness command to Arduino"""
    command = f"B:{brightness}\n"
    ser.write(command.encode())
    print(f"💡 Sent to Arduino → {command.strip()}")
 
 
# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
 
def main():
    validate_config()
 
    mode_label = "🟡 DEBUG (15s)" if DEBUG_MODE else "🟢 PRODUCTION (90s)"
    print(f"\n{'='*55}")
    print(f"  LumiSense Grid — Python Bridge")
    print(f"  Mode          : {mode_label}")
    print(f"  Batch Size    : {BATCH_SIZE} readings")
    print(f"  Port          : {SERIAL_PORT}")
    print(f"  Cache File    : {CACHE_FILE}")
    print(f"  Cache Refresh : every day at "
          f"{CACHE_REFRESH_HOUR}:{CACHE_REFRESH_MIN:02d} AM")
    print(f"  Light ID      : from Arduino (not hardcoded)")
    print(f"{'='*55}\n")
 
    # ── Connect Snowflake ──────────────────────────────────────
    print("🔌 Connecting to Snowflake...")
    conn   = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    print("✅ Snowflake connected!\n")
 
    # ── Initialize rules cache ─────────────────────────────────
    rules_cache = initialize_cache(cursor)
 
    # ── Connect Arduino ────────────────────────────────────────
    print(f"\n🔌 Connecting to Arduino on {SERIAL_PORT}...")
    ser = serial.Serial(SERIAL_PORT, BAUD_RATE, timeout=5)
    time.sleep(2)
    print("✅ Arduino connected!")
 
    # ── Get Light ID from Arduino ──────────────────────────────
    light_id    = wait_for_light_id(ser)
 
    ldr_buffer   = []
    reading_num  = 0
    startup_sent = False   # track if initial brightness sent
 
    print(f"\n🚀 Bridge running — Light ID: {light_id}\n")
 
    while True:
        try:
            # ── Refresh cache at 2:10 AM daily ────────────────
            rules_cache = refresh_cache_if_needed(cursor, rules_cache)
 
            # ── Read from Arduino ──────────────────────────────
            raw = ser.readline().decode().strip()
 
            if not raw:
                continue
 
            # Skip non-data lines (LED confirmations, debug msgs)
            if raw.startswith("LED") or raw.startswith("DEBUG"):
                print(f"ℹ️  Arduino: {raw}")
                continue
 
            # Re-identify if Arduino restarted mid-session
            if raw.startswith("ID:"):
                light_id = raw.split(":")[1]
                print(f"🔄 Arduino re-identified as: {light_id}")
                continue
 
            # Parse "L1:450" — light_id:ldr_value
            if ":" in raw:
                parts     = raw.split(":")
                light_id  = parts[0]
                ldr_value = int(parts[1])
            else:
                continue
 
            hour_of_day  = datetime.now().hour
            reading_num += 1
            print(f"\n[{reading_num}] 📡 Light: {light_id} | "
                  f"LDR: {ldr_value} | Hour: {hour_of_day} | "
                  f"Buffer: {len(ldr_buffer)+1}/{BATCH_SIZE}")
 
            # ── Write LDR to Snowflake ─────────────────────────
            write_LDR_GRID_DATA(cursor, conn, light_id, ldr_value, hour_of_day)
            ldr_buffer.append(ldr_value)
 
            # ── Startup: send brightness immediately on first reading ──
            if not startup_sent:
                print("⚡ Startup — sending initial brightness before batch fills...")
                decision = read_LDR_LED_SCENARIO_RULES(
                    rules_cache, ldr_value, hour_of_day
                )
                send_to_arduino(ser, decision['brightness'])
                startup_sent = True
 
            # ── Batch full → lookup from CSV cache ────────────
            elif len(ldr_buffer) >= BATCH_SIZE:
                avg_ldr = round(sum(ldr_buffer) / len(ldr_buffer))
                print(f"\n🔎 Batch complete — Avg LDR: {avg_ldr}")
 
                decision = read_LDR_LED_SCENARIO_RULES(
                    rules_cache, avg_ldr, hour_of_day
                )
 
                send_to_arduino(ser, decision['brightness'])
                ldr_buffer = []
 
        except KeyboardInterrupt:
            print("\n🛑 Stopped.")
            break
        except Exception as e:
            print(f"❌ Error: {e}")
            continue
 
    cursor.close()
    conn.close()
    ser.close()
    print("👋 Connections closed.")
 
 
if __name__ == "__main__":
    main()