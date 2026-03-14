import serial
import snowflake.connector
import time
import os
from datetime import datetime
from dotenv import load_dotenv

# ─── CONFIG ───────────────────────────────────────
SERIAL_PORT = 'COM5'
BAUD_RATE = 9600

DEBUG_MODE  = True
BATCH_SIZE  = 3 if DEBUG_MODE else 10
# ──────────────────────────────────────────────────

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

# ─── FUNCTION 1: Write LDR reading to Snowflake ───────────────
def write_LDR_GRID_DATA(cursor, conn, light_id, ldr_value, hour_of_day):
    cursor.execute("""
        INSERT INTO LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_GRID_DATA
            (light_id, ldr_value, hour_of_day)
        VALUES (%s, %s, %s)
    """, (light_id, ldr_value, hour_of_day))
    conn.commit()
    print(f"✅ LDR saved → Light: {light_id} | LDR: {ldr_value} | Hour: {hour_of_day}")


# ─── FUNCTION 2: Read brightness decision from rules table ────
def read_LDR_LED_SCENARIO_RULES(cursor, ldr_value, hour_of_day):
    cursor.execute("""
        SELECT brightness_pct, energy_mode, saving_pct
        FROM LUMISENSE_DB.LUMISENSE_SCHEMA.LDR_LED_SCENARIO_RULES
        WHERE %s >= ldr_min
          AND %s <= ldr_max
          AND %s >= hour_min
          AND %s <= hour_max
          AND light_id = 'ALL'
        LIMIT 1
    """, (ldr_value, ldr_value, hour_of_day, hour_of_day))

    row = cursor.fetchone()
    if row:
        print(f"📊 Rule matched → Brightness: {row[0]}% | Mode: {row[1]} | Saving: {row[2]}%")
        return {'brightness': row[0], 'mode': row[1], 'saving': row[2]}
    else:
        print("⚠️  No rule matched — using default 50%")
        return {'brightness': 50, 'mode': 'NORMAL', 'saving': 30}


# ─── FUNCTION 3: Send brightness command to Arduino ───────────
def send_to_arduino(ser, brightness):
    command = f"B:{brightness}\n"
    ser.write(command.encode())
    print(f"💡 Sent to Arduino → {command.strip()}")


# ─── MAIN LOOP ────────────────────────────────────────────────
def main():
    mode_label = "🟡 DEBUG MODE (15s interval)" if DEBUG_MODE else "🟢 PRODUCTION MODE (90s interval)"
    print(f"\n{'='*50}")
    print(f"  LumiSense Grid — Python Bridge")
    print(f"  {mode_label}")
    print(f"  Batch Size : {BATCH_SIZE} readings")
    print(f"  Decision every ~{BATCH_SIZE * (15 if DEBUG_MODE else 90)}s")
    print(f"{'='*50}\n")

    print("🔌 Connecting to Snowflake...")
    conn   = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    print("✅ Snowflake connected!\n")

    print(f"🔌 Connecting to Arduino on {SERIAL_PORT}...")
    ser = serial.Serial(SERIAL_PORT, BAUD_RATE, timeout=5)
    time.sleep(2)
    print("✅ Arduino connected!\n")

    ldr_buffer  = []
    reading_num = 0

    while True:
        try:
            # ── Step 1: Read LDR from Arduino ─────────────────
            raw = ser.readline().decode().strip()

            # Skip non-numeric lines (e.g. "DEBUG:ON", "LED set to: X")
            if not raw.isdigit():
                print(f"ℹ️  Arduino: {raw}")
                continue

            ldr_value   = int(raw)
            hour_of_day = datetime.now().hour
            reading_num += 1
            print(f"\n[{reading_num}] 📡 LDR: {ldr_value} | Hour: {hour_of_day} | "
                  f"Buffer: {len(ldr_buffer)+1}/{BATCH_SIZE}")

            # ── Step 2: Write to Snowflake ────────────────────
            write_LDR_GRID_DATA(cursor, conn, LIGHT_ID, ldr_value, hour_of_day)
            ldr_buffer.append(ldr_value)

            # ── Step 3: Batch full → get decision ─────────────
            if len(ldr_buffer) >= BATCH_SIZE:
                avg_ldr = round(sum(ldr_buffer) / len(ldr_buffer))
                print(f"\n🔎 Batch complete — Avg LDR: {avg_ldr}")

                decision = read_LDR_LED_SCENARIO_RULES(cursor, avg_ldr, hour_of_day)

                # ── Step 4: Send brightness back to Arduino ───
                send_to_arduino(ser, decision['brightness'])

                ldr_buffer = []

        except KeyboardInterrupt:
            print("\n🛑 Stopped by user.")
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