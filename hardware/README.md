# Hardware design status

The software fleet is operational. This directory is an engineering design correction, not a photograph, Tinkercad simulation result or physical-device validation.

The submitted plan has three hardware assumptions that must be corrected before circuit sign-off:

1. DHT22 provides temperature and humidity through one digital data connection, not separate A0/A1 analog signals. Its maximum fresh sample rate is0.5Hz. The1Hz fleet tests use synthetic readings; a physical1Hz publisher must carry sensor sample-age metadata or use a faster sensor.
2. One PIR detects motion, not a numeric headcount. occupancyCount in this prototype is synthetic. A physical deployment needs a separately validated counting method, or a documented binary-presence control rule.
3. An MQ-style gas sensor is not an interchangeable source of calibrated CO2 ppm. The proposed physical option is an SCD41 photoacoustic NDIR CO2 sensor on I2C; its actual measurement cadence must be reflected in sample freshness handling.

`device-manifest.json` records the corrected proposed interface. It does not claim that ESP32 or these exact components have been built inside Tinkercad. The original plan's circuit remains a pending milestone until a real supported circuit is built and exercised.

Sources: [Adafruit DHT overview](https://learn.adafruit.com/dht/overview), [Adafruit PIR guide](https://learn.adafruit.com/pir-passive-infrared-proximity-motion-sensor?view=all), [Sensirion SCD41](https://sensirion.com/products/catalog/SCD41).
