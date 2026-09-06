export class Gateway {
  constructor({ bus }) {
    this.bus = bus;
    this.accepted = 0;
    this.rejected = 0;
    this.unsubscribe = bus.subscribe('device.telemetry.raw', (event) => this.handleRawTelemetry(event));
  }

  handleRawTelemetry(event) {
    const reading = event.payload;
    const validationError = validateReading(reading);

    if (validationError) {
      this.rejected += 1;
      this.bus.publish('gateway.telemetry.rejected', {
        reason: validationError,
        sourceEventId: event.id
      });
      return;
    }

    this.accepted += 1;
    this.bus.publish('sensor.reading', {
      ...reading,
      receivedAt: event.timestamp,
      normalizedBy: 'edge-gateway-01'
    });
  }

  snapshot() {
    return {
      accepted: this.accepted,
      rejected: this.rejected
    };
  }

  close() {
    this.unsubscribe();
  }
}

function validateReading(reading) {
  if (!reading.deviceId || !reading.zoneId) {
    return 'missing device or zone id';
  }

  if (!Number.isFinite(reading.temperatureC) || reading.temperatureC < -20 || reading.temperatureC > 80) {
    return 'temperature outside expected range';
  }

  if (!Number.isFinite(reading.occupancyCount) || reading.occupancyCount < 0) {
    return 'occupancy outside expected range';
  }

  return null;
}
