import snowflake.connector
import random
import uuid
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

# ─── LOAD .env ────────────────────────────────────────────────
load_dotenv()
 
# ─── CONFIG FROM .env ─────────────────────────────────────────
SNOWFLAKE_CONFIG = {
    'user':      os.getenv('SNOWFLAKE_USER'),
    'password':  os.getenv('SNOWFLAKE_PASSWORD'),
    'account':   os.getenv('SNOWFLAKE_ACCOUNT'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'database':  os.getenv('SNOWFLAKE_DATABASE'),
    'schema':    os.getenv('SNOWFLAKE_SCHEMA'),
}


# ─── ZONES ────────────────────────────────────────
ZONES = {
    "Residential": {
        "lights": ["L1","L2","L3","L4","L5"],
        "type": "Sub-Urban",
        "ldr_offset": 0
    },
    "Main Road": {
        "lights": ["L6","L7","L8","L9","L10"],
        "type": "Urban",
        "ldr_offset": 50
    },
    "Park": {
        "lights": ["L11","L12","L13","L14","L15"],
        "type": "Green",
        "ldr_offset": 100
    },
    "Industrial": {
        "lights": ["L16","L17","L18","L19","L20"],
        "type": "Commercial",
        "ldr_offset": -80
    }
}

# Per light small variation for realism
LIGHT_VARIATION = {f"L{i}": random.randint(-25, 25) for i in range(1, 21)}

# ─── LOOKUP TABLE (mirrors brightness_rules) ──────
RULES = [
    (1,  0,   299,  0,  5,  50,  'ECO',    70),
    (2,  0,   299,  6,  8,  95,  'FULL',   10),
    (3,  0,   299,  9,  17, 80,  'NORMAL', 30),
    (4,  0,   299,  18, 21, 100, 'FULL',   5),
    (5,  0,   299,  22, 23, 60,  'NORMAL', 50),
    (6,  300, 599,  0,  5,  20,  'ECO',    80),
    (7,  300, 599,  6,  8,  60,  'NORMAL', 40),
    (8,  300, 599,  9,  17, 30,  'ECO',    60),
    (9,  300, 599,  18, 21, 80,  'FULL',   20),
    (10, 300, 599,  22, 23, 35,  'NORMAL', 55),
    (11, 600, 1023, 0,  5,  10,  'ECO',    90),
    (12, 600, 1023, 6,  8,  30,  'ECO',    65),
    (13, 600, 1023, 9,  17, 5,   'ECO',    95),
    (14, 600, 1023, 18, 21, 50,  'NORMAL', 45),
    (15, 600, 1023, 22, 23, 15,  'ECO',    80),
]

SCENARIO_LABELS = {
    1:  "Dark Night — ECO",
    2:  "Dark Dawn — Full Power",
    3:  "Dark Daytime — Normal",
    4:  "Dark Evening — Full Power",
    5:  "Dark Late Night — Normal",
    6:  "Moderate Night — ECO",
    7:  "Moderate Morning — Normal",
    8:  "Moderate Daytime — ECO",
    9:  "Moderate Evening — Full",
    10: "Moderate Late Night — Normal",
    11: "Bright Night — Deep ECO",
    12: "Bright Morning — ECO",
    13: "Bright Daytime — Deep ECO",
    14: "Bright Evening — Normal",
    15: "Bright Late Night — ECO",
}

# ─── HELPERS ──────────────────────────────────────

def get_ldr(hour, zone):
    offset = ZONES[zone]["ldr_offset"]
    if 0 <= hour <= 5:    base = random.randint(30, 150)
    elif 6 <= hour <= 8:  base = random.randint(200, 500)
    elif 9 <= hour <= 17: base = random.randint(600, 950)
    elif 18 <= hour <= 21:base = random.randint(250, 550)
    else:                 base = random.randint(80, 250)
    return max(0, min(1023, base + offset + random.randint(-25, 25)))

def get_ldr_category(ldr):
    if ldr < 300:   return "Dark"
    elif ldr < 600: return "Moderate"
    else:           return "Bright"

def get_time_period(hour):
    if 0 <= hour <= 5:    return "Midnight"
    elif 6 <= hour <= 8:  return "Morning"
    elif 9 <= hour <= 17: return "Daytime"
    elif 18 <= hour <= 21:return "Evening"
    else:                  return "Late Night"

def get_rule(ldr, hour):
    for r in RULES:
        if r[1] <= ldr <= r[2] and r[3] <= hour <= r[4]:
            return r[0], r[5], r[6], r[7]
    return 1, 50, 'NORMAL', 30

def build_row(dt, light_id, zone):
    hour      = dt.hour
    ldr       = get_ldr(hour, zone)
    ldr       = max(0, min(1023, ldr + LIGHT_VARIATION.get(light_id, 0)))
    scenario_id, brightness, mode, saving = get_rule(ldr, hour)

    # Power calculations
    wattage       = 150.0                             # standard 150W street light
    actual_w      = round(wattage * brightness / 100, 2)
    power_saved   = round(wattage - actual_w, 2)
    cost_kwh      = 0.12                              # $0.12 per kWh
    interval_hrs  = 0.25                              # 15 min = 0.25 hr
    cost_saved    = round(power_saved * interval_hrs / 1000 * cost_kwh, 5)
    co2_saved     = round(power_saved * interval_hrs / 1000 * 0.233, 5)  # 0.233 kg/kWh

    # Grid health simulation
    voltage       = round(random.uniform(228, 238), 1)
    current       = round(actual_w / voltage, 3)
    grid_load     = round((actual_w / wattage) * 100, 1)

    day_name      = dt.strftime("%A")
    is_weekend    = day_name in ("Saturday", "Sunday")
    month_name    = dt.strftime("%B")

    return (
        str(uuid.uuid4()),              # fact_id
        dt.strftime('%Y-%m-%d %H:%M:%S'),  # snapshot_time
        light_id,                       # light_id
        zone,                           # zone
        ZONES[zone]["type"],            # zone_type
        ldr,                            # ldr_value
        get_ldr_category(ldr),          # ldr_category
        hour,                           # hour_of_day
        get_time_period(hour),          # time_period
        day_name,                       # day_of_week
        is_weekend,                     # is_weekend
        month_name,                     # month_name
        brightness,                     # brightness_pct
        mode,                           # energy_mode
        saving,                         # saving_pct
        wattage,                        # wattage_per_light
        actual_w,                       # actual_wattage
        power_saved,                    # power_saved_w
        cost_kwh,                       # cost_per_kwh
        cost_saved,                     # cost_saved_usd
        co2_saved,                      # co2_saved_kg
        voltage,                        # voltage_v
        current,                        # current_a
        grid_load,                      # grid_load_pct
        scenario_id,                    # scenario_id
        SCENARIO_LABELS[scenario_id],   # scenario_label
    )

# ─── MAIN ─────────────────────────────────────────

def main():
    print("🔌 Connecting to Snowflake...")
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    print("✅ Connected!\n")

    # Clear existing data
    print("🗑️  Clearing existing fact_dashboard data...")
    cursor.execute("DELETE FROM LUMISENSE_DB.LUMISENSE_SCHEMA.fact_dashboard")
    conn.commit()

    # Build rows — 30 days, every 15 minutes, 20 lights
    print("🏗️  Building fact table rows...")
    print("    30 days × 96 intervals × 20 lights = 57,600 rows\n")

    now       = datetime.now().replace(second=0, microsecond=0)
    # Round to last 15 min mark
    now       = now - timedelta(minutes=now.minute % 15)
    start     = now - timedelta(days=30)

    rows = []
    current_time = start

    while current_time <= now:
        for zone, data in ZONES.items():
            for light_id in data["lights"]:
                rows.append(build_row(current_time, light_id, zone))
        current_time += timedelta(minutes=15)

    print(f"✅ {len(rows):,} rows generated!")
    print(f"📦 Inserting in batches of 2,000...\n")

    # Batch insert
    chunk_size = 2000
    total_chunks = (len(rows) + chunk_size - 1) // chunk_size

    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i + chunk_size]
        cursor.executemany("""
            INSERT INTO LUMISENSE_DB.LUMISENSE_SCHEMA.fact_dashboard (
                fact_id, snapshot_time, light_id, zone, zone_type,
                ldr_value, ldr_category, hour_of_day, time_period,
                day_of_week, is_weekend, month_name,
                brightness_pct, energy_mode, saving_pct,
                wattage_per_light, actual_wattage, power_saved_w,
                cost_per_kwh, cost_saved_usd, co2_saved_kg,
                voltage_v, current_a, grid_load_pct,
                scenario_id, scenario_label
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s
            )
        """, chunk)
        conn.commit()
        chunk_num = i // chunk_size + 1
        pct = round(chunk_num / total_chunks * 100)
        print(f"   [{pct:>3}%] Chunk {chunk_num}/{total_chunks} inserted "
              f"({min(i+chunk_size, len(rows)):,}/{len(rows):,} rows)")

    # Verify
    cursor.execute("""
        SELECT COUNT(*), MIN(snapshot_time), MAX(snapshot_time)
        FROM LUMISENSE_DB.LUMISENSE_SCHEMA.fact_dashboard
    """)
    result = cursor.fetchone()
    print(f"\n📊 Verification:")
    print(f"   Total rows  : {result[0]:,}")
    print(f"   From        : {result[1]}")
    print(f"   To          : {result[2]}")

    cursor.close()
    conn.close()
    print("\n🎉 fact_dashboard populated successfully!")
    print("💡 Now run lumisense_dashboard_v2.sql in Snowsight!")

if __name__ == "__main__":
    main()