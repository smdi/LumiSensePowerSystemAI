int ldrPin = A4;   // LDR connected to analog pin A0
int ldrValue = 0;

void setup() {
  Serial.begin(9600);   // start serial communication
}

void loop() {

  ldrValue = analogRead(ldrPin);   // read LDR value

  Serial.print("LDR Reading: ");
  Serial.println(ldrValue);        // print value to serial monitor

  delay(1000);   // wait half second
}
