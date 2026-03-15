import snowflake.connector
import random
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
# Each zone has different natural light levels
ZONES = {
    "residential": {
        "lights": ["L1", "L2", "L3", "L4", "L5"],
        "offset": 0,
        "description": "Residential streets — moderate light"
    },
    "main_road": {
        "lights": ["L6", "L7", "L8", "L9", "L10"],
        "offset": 50,
        "description": "Main roads — open sky, brighter"
    },
    "park": {
        "lights": ["L11", "L12", "L13", "L14", "L15"],
        "offset": 100,
        "description": "Park area — most open, brightest"
    },
    "industrial": {
        "lights": ["L16", "L17", "L18", "L19", "L20"],
        "offset": -80,
        "description": "Industrial zone — covered, darker"
    },
}

# Each light has a small personal variation
LIGHT_VARIATION = {f"L{i}": random.randint(-30, 30) for i in range(1, 21)}

# ─── HELPERS ──────────────────────────────────────

def get_zone(light_id):
    for zone, data in ZONES.items():
        if light_id in data["lights"]:
            return zone
    return "residential"

def get_ldr(hour, light_id):
    """Realistic LDR based on time + zone + light variation"""
    zone = get_zone(light_id)
    offset = ZONES[zone]["offset"] + LIGHT_VARIATION[light_id]

    if 0 <= hour <= 5:
        base = random.randint(30, 150)
    elif 6 <= hour <= 8:
        base = random.randint(200, 500)
    elif 9 <= hour <= 17:
        base = random.randint(600, 950)
    elif 18 <= hour <= 21:
        base = random.randint(250, 550)
    else:  # 22-23
        base = random.randint(80, 250)

    # Clamp between 0 and 1023
    return max(0, min(1023, base + offset))

def get_rule(ldr, hour, rules):
    """Match LDR + hour to brightness rule"""
    for rule in rules:
        # rule = (scenario_id, ldr_min, ldr_max, hour_min, hour_max,
        #          brightness_pct, energy_mode, saving_pct)
        if (rule[1] <= ldr <= rule[2] and
                rule[3] <= hour <= rule[4]):
            return rule[5], rule[6], rule[7]
    return 50, 'NORMAL', 30  # fallback

# ─── POPULATE FUNCTIONS ───────────────────────────

def populate_street_grid_data(cursor, conn):
    """
    3 days of readings for 20 lights
    1 reading per light per 90 seconds
    = ~960 readings per light = ~19,200 total rows
    """
    print("\n📡 Populating street_grid_data...")

    lights = [f"L{i}" for i in range(1, 21)]
    rows = []

    now = datetime.now()
    three_days_ago = now - timedelta(days=3)

    # Step every 90 seconds for 3 days
    current_time = three_days_ago
    while current_time <= now:
        hour = current_time.hour
        for light_id in lights:
            ldr = get_ldr(hour, light_id)
            rows.append((light_id, ldr, hour,
                         current_time.strftime('%Y-%m-%d %H:%M:%S')))
        current_time += timedelta(seconds=90)

    print(f"   → Inserting {len(rows):,} rows...")

    # Batch insert in chunks of 1000
    chunk_size = 1000
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i+chunk_size]
        cursor.executemany("""
            INSERT INTO LUMISENSE_DB.LUMISENSE_SCHEMA.street_grid_data
                (light_id, ldr_value, hour_of_day, timestamp)
            VALUES (%s, %s, %s, %s)
        """, chunk)
        conn.commit()
        print(f"   → {min(i+chunk_size, len(rows)):,}/{len(rows):,} inserted...")

    print(f"✅ street_grid_data done! ({len(rows):,} rows)")


def populate_ai_decisions(cursor, conn):
    """
    Decision every 15 min for 20 lights over 3 days
    = ~576 decisions per light = ~11,520 total rows
    """
    print("\n🔍 Populating ai_decisions...")

    # Get current rules from Snowflake
    cursor.execute("""
        SELECT scenario_id, ldr_min, ldr_max,
               hour_min, hour_max,
               brightness_pct, energy_mode, saving_pct
        FROM LUMISENSE_DB.LUMISENSE_SCHEMA.brightness_rules
        ORDER BY scenario_id
    """)
    rules = cursor.fetchall()

    lights = [f"L{i}" for i in range(1, 21)]
    rows = []

    now = datetime.now()
    three_days_ago = now - timedelta(days=3)

    # Step every 15 minutes
    current_time = three_days_ago
    while current_time <= now:
        hour = current_time.hour
        for light_id in lights:
            # Average of 10 readings = realistic avg LDR
            ldr_samples = [get_ldr(hour, light_id) for _ in range(10)]
            avg_ldr = round(sum(ldr_samples) / len(ldr_samples))

            brightness, mode, saving = get_rule(avg_ldr, hour, rules)
            rows.append((
                light_id, avg_ldr, hour,
                brightness, mode, saving,
                current_time.strftime('%Y-%m-%d %H:%M:%S')
            ))
        current_time += timedelta(minutes=15)

    print(f"   → Inserting {len(rows):,} rows...")

    chunk_size = 1000
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i+chunk_size]
        cursor.executemany("""
            INSERT INTO LUMISENSE_DB.LUMISENSE_SCHEMA.ai_decisions
                (light_id, ldr_value, hour_of_day,
                 brightness_pct, energy_mode, monthly_saving_pct,
                 timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, chunk)
        conn.commit()
        print(f"   → {min(i+chunk_size, len(rows)):,}/{len(rows):,} inserted...")

    print(f"✅ ai_decisions done! ({len(rows):,} rows)")


def populate_insights_log(cursor, conn):
    """
    1 insight per hour for 3 days = 72 rows
    """
    print("\n💡 Populating insights_log...")

    rows = []
    now = datetime.now()
    three_days_ago = now - timedelta(days=3)

    # Realistic insight templates (no activity sensor)
    insight_templates = [
        "1. Energy saving at {s}% — ECO mode dominant overnight.\n2. LDR peaks {p} between 9-17hrs — daytime fully dimmed.\n3. Reduce scenario 8 brightness by 5% to save more.",
        "1. Average saving {s}% — strong ECO performance.\n2. LDR drops sharply after 22hrs — late night very dark.\n3. Scenario 11 brightness could reduce from 10% to 5%.",
        "1. {s}% energy saved this hour — well optimized.\n2. LDR readings consistent — stable weather conditions.\n3. Hour 0-5 savings highest — maintain current ECO rules.",
        "1. Saving trend stable at {s}% — rules working well.\n2. LDR highest at midday — all lights in ECO mode.\n3. Evening hours 18-21 use most power — expected pattern.",
        "1. ECO mode active {e} times — excellent efficiency.\n2. Low LDR after midnight — streets naturally dark.\n3. Morning hours 6-8 spike in brightness — correct behavior.",
    ]

    current_time = three_days_ago
    while current_time <= now:
        hour = current_time.hour

        # Realistic stats per hour
        avg_ldr = round(get_ldr(hour, "L1") * 0.9 + random.randint(-20, 20))
        avg_brightness = random.randint(10, 60)
        avg_saving = 100 - avg_brightness
        eco_count = random.randint(8, 18)
        full_count = random.randint(0, 5)

        template = random.choice(insight_templates)
        insight = template.format(
            s=avg_saving,
            p=round(avg_ldr),
            e=eco_count
        )

        rows.append((
            round(avg_ldr, 2),
            round(avg_brightness, 2),
            round(avg_saving, 2),
            eco_count,
            full_count,
            insight,
            current_time.strftime('%Y-%m-%d %H:%M:%S')
        ))
        current_time += timedelta(hours=1)

    cursor.executemany("""
        INSERT INTO LUMISENSE_DB.LUMISENSE_SCHEMA.insights_log
            (avg_ldr, avg_brightness, avg_saving,
             eco_count, full_count, insights, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, rows)
    conn.commit()
    print(f"✅ insights_log done! ({len(rows)} rows)")


def populate_optimization_log(cursor, conn):
    """
    A few realistic optimization changes over 3 days
    """
    print("\n🧠 Populating optimization_log...")

    rows = [
        (6,  20, 15, 80, 85,
         "avg_ldr 580 near ldr_max 599 — safe to dim",
         (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d %H:%M:%S')),

        (8,  30, 25, 60, 65,
         "avg_ldr 570 consistently high — reducing brightness",
         (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d %H:%M:%S')),

        (13, 5,  5,  95, 95,
         "already at minimum — no change applied",
         (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')),

        (11, 10, 8,  90, 92,
         "midnight readings show LDR 600+ stable — dimming safely",
         (datetime.now() - timedelta(hours=12)).strftime('%Y-%m-%d %H:%M:%S')),

        (15, 15, 12, 80, 83,
         "late night LDR near max — small brightness reduction",
         (datetime.now() - timedelta(hours=6)).strftime('%Y-%m-%d %H:%M:%S')),
    ]

    cursor.executemany("""
        INSERT INTO LUMISENSE_DB.LUMISENSE_SCHEMA.optimization_log
            (scenario_id, old_brightness, new_brightness,
             old_saving, new_saving, reason, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, rows)
    conn.commit()
    print(f"✅ optimization_log done! ({len(rows)} rows)")


def verify_data(cursor):
    """Quick count check on all tables"""
    print("\n📊 Verifying data...")
    tables = [
        "street_grid_data",
        "ai_decisions",
        "insights_log",
        "optimization_log",
        "brightness_rules"
    ]
    for table in tables:
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM LUMISENSE_DB.LUMISENSE_SCHEMA.{table}
        """)
        count = cursor.fetchone()[0]
        print(f"   {table:<25} → {count:>8,} rows")


# ─── MAIN ─────────────────────────────────────────

def main():
    print("🔌 Connecting to Snowflake...")
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    print("✅ Connected!\n")

    print("🗂️  Zone breakdown:")
    for zone, data in ZONES.items():
        print(f"   {zone:<15} → {data['lights']} — {data['description']}")

    populate_street_grid_data(cursor, conn)
    populate_ai_decisions(cursor, conn)
    populate_insights_log(cursor, conn)
    populate_optimization_log(cursor, conn)
    verify_data(cursor)

    cursor.close()
    conn.close()
    print("\n🎉 All tables populated successfully!")
    print("💡 Set DEBUG_MODE = True in lumisense.py to test insights & optimization!")

if __name__ == "__main__":
    main()