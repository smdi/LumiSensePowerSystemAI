int ldrPin     = A0;
int ledPin     = 6;
int ldrValue   = 0;
int brightness = 0;

// ── DEVICE IDENTITY ─────────────────────────────
const String LIGHT_ID = "ALL";
// ─────────────────────────────────────────────────

// ── DEBUG MODE ──────────────────────────────────
// true  = send every 15 seconds (testing)
// false = send every 90 seconds (production)
bool DEBUG_MODE = true;
// ─────────────────────────────────────────────────

void setup() {
  pinMode(ledPin, OUTPUT);
  Serial.begin(9600);
  delay(1000);

  // Send identity first so Python knows which light this is
  Serial.println("ID:" + LIGHT_ID);

  if (DEBUG_MODE) {
    Serial.println("DEBUG:ON");
  }
}

void loop() {
  ldrValue = analogRead(ldrPin);

  // Send to python bridge
  Serial.println(LIGHT_ID + ":" + String(ldrValue));

  if (Serial.available() > 0) {
    String command = Serial.readStringUntil('\n');
    command.trim();
    if (command.startsWith("B:")) {
      brightness = command.substring(2).toInt();
      brightness = map(brightness, 0, 100, 0, 255);
      analogWrite(ledPin, brightness);
      Serial.print("LED set to: ");
      Serial.println(brightness);
    }
  }

  // 15s in debug, 90s in production
  delay(DEBUG_MODE ? 15000 : 90000);
}
