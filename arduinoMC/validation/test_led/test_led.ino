int ledPin = 6;

void setup() {
  pinMode(ledPin, OUTPUT);
  Serial.begin(9600);
  Serial.println("=== LumiSense LED Demo ===");
}

void loop() {

  // ── SCENARIO: BRIGHT DAYTIME ──────────────────
  // LDR reading high → lights dim → saving energy
  Serial.println("Scenario: Bright Daytime (ECO Mode)");
  Serial.println("LDR: 750 | Brightness: 5% | Saving: 95%");
  analogWrite(ledPin, map(5, 0, 100, 0, 255));
  delay(4000);

  // ── SCENARIO: MODERATE EVENING ────────────────
  // LDR reading moderate → medium brightness
  Serial.println("Scenario: Moderate Evening (NORMAL Mode)");
  Serial.println("LDR: 420 | Brightness: 35% | Saving: 65%");
  analogWrite(ledPin, map(35, 0, 100, 0, 255));
  delay(4000);

  // ── SCENARIO: DARK EVENING PEAK ───────────────
  // LDR reading low during peak hours → full brightness
  Serial.println("Scenario: Dark Evening Peak (FULL Mode)");
  Serial.println("LDR: 150 | Brightness: 100% | Saving: 0%");
  analogWrite(ledPin, map(100, 0, 100, 0, 255));
  delay(4000);

  // ── SCENARIO: DARK MIDNIGHT ───────────────────
  // Dark but low traffic → ECO mode
  Serial.println("Scenario: Dark Midnight (ECO Mode)");
  Serial.println("LDR: 80 | Brightness: 50% | Saving: 50%");
  analogWrite(ledPin, map(50, 0, 100, 0, 255));
  delay(4000);

  // ── SCENARIO: BRIGHT MIDNIGHT ─────────────────
  // Ambient light high even at night → deep ECO
  Serial.println("Scenario: Bright Midnight (Deep ECO Mode)");
  Serial.println("LDR: 680 | Brightness: 10% | Saving: 90%");
  analogWrite(ledPin, map(10, 0, 100, 0, 255));
  delay(4000);

  Serial.println("─────────────────────────────────");
}
