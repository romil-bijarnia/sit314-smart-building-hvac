export class AggregationService {
  constructor({ bus, windowSize = 10 }) {
    this.bus = bus;
    this.windowSize = windowSize;
    this.zoneWindows = new Map();
    this.zoneMetrics = new Map();
    this.unsubscribe = bus.subscribe('sensor.reading', (event) => this.handleReading(event));
  }

  handleReading(event) {
    const reading = event.payload;
    const readings = this.zoneWindows.get(reading.zoneId) ?? [];
    readings.push(reading);

    if (readings.length > this.windowSize) {
      readings.shift();
    }

    this.zoneWindows.set(reading.zoneId, readings);
    const metrics = calculateMetrics(reading.zoneId, readings);
    this.zoneMetrics.set(reading.zoneId, metrics);
    this.bus.publish('zone.metrics.updated', metrics);
  }

  snapshot() {
    return Array.from(this.zoneMetrics.values()).sort((left, right) => left.zoneId.localeCompare(right.zoneId));
  }

  close() {
    this.unsubscribe();
  }
}

function calculateMetrics(zoneId, readings) {
  const count = readings.length || 1;
  const sum = readings.reduce(
    (total, reading) => ({
      temperatureC: total.temperatureC + reading.temperatureC,
      humidityPercent: total.humidityPercent + reading.humidityPercent,
      occupancyCount: total.occupancyCount + reading.occupancyCount,
      co2Ppm: total.co2Ppm + reading.co2Ppm
    }),
    { temperatureC: 0, humidityPercent: 0, occupancyCount: 0, co2Ppm: 0 }
  );

  return {
    zoneId,
    sampleCount: count,
    averageTemperatureC: round(sum.temperatureC / count),
    averageHumidityPercent: round(sum.humidityPercent / count),
    averageOccupancyCount: round(sum.occupancyCount / count),
    averageCo2Ppm: Math.round(sum.co2Ppm / count)
  };
}

function round(value) {
  return Math.round(value * 100) / 100;
}
