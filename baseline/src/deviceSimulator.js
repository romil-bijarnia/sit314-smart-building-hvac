import { buildDeviceManifest } from './hardwareLab.js';

export class DeviceFleet {
  constructor({ bus, buildingId, deviceCount, zoneCount, intervalMs }) {
    this.bus = bus;
    this.manifest = buildDeviceManifest({ buildingId, deviceCount, zoneCount });
    this.devices = this.manifest.map((device, index) => {
      return new SimulatedRoomSensor({
        bus,
        device,
        intervalMs,
        seed: index + 1
      });
    });
  }

  start() {
    for (const device of this.devices) {
      device.start();
    }
  }

  stop() {
    for (const device of this.devices) {
      device.stop();
    }
  }

  snapshot() {
    return {
      devices: this.manifest.length,
      zones: new Set(this.manifest.map((device) => device.zoneId)).size,
      communication: 'MQTT over Wi-Fi',
      sensorTypes: ['temperature', 'humidity', 'occupancy', 'co2'],
      actuatorTypes: ['HVAC damper and fan actuator'],
      manifest: this.manifest.slice(0, 10)
    };
  }
}

class SimulatedRoomSensor {
  constructor({ bus, device, intervalMs, seed }) {
    this.bus = bus;
    this.device = device;
    this.intervalMs = intervalMs;
    this.seed = seed;
    this.tick = 0;
    this.timer = null;
  }

  start() {
    this.emitReading();
    this.timer = setInterval(() => this.emitReading(), this.intervalMs);
  }

  stop() {
    if (this.timer) {
      clearInterval(this.timer);
    }
  }

  emitReading() {
    this.tick += 1;
    const cycle = Math.sin((this.tick + this.seed) / 8);
    const occupancy = Math.max(0, Math.round(8 + 6 * Math.sin((this.tick + this.seed) / 5)));
    const heatLoad = occupancy * 0.11;
    const randomNoise = pseudoRandom(this.seed, this.tick) * 0.8 - 0.4;

    this.bus.publish('device.telemetry.raw', {
      protocol: 'simulated-mqtt',
      topic: this.device.telemetryTopic,
      buildingId: this.device.buildingId,
      deviceId: this.device.deviceId,
      controllerId: this.device.controllerId,
      zoneId: this.device.zoneId,
      room: this.device.room,
      board: this.device.board,
      communication: this.device.communication,
      temperatureC: round(22.5 + cycle * 3 + heatLoad + randomNoise),
      humidityPercent: round(48 + cycle * 8 + randomNoise),
      occupancyCount: occupancy,
      co2Ppm: Math.round(420 + occupancy * 38 + randomNoise * 20),
      batteryPercent: Math.max(30, 100 - this.tick * 0.01)
    });
  }
}

function pseudoRandom(seed, tick) {
  const x = Math.sin(seed * 1000 + tick * 77) * 10000;
  return x - Math.floor(x);
}

function round(value) {
  return Math.round(value * 100) / 100;
}
