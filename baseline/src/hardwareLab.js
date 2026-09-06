export const sensorTypes = {
  temperature: {
    component: 'DHT22 temperature sensor',
    unit: 'degrees Celsius',
    purpose: 'Measures zone thermal comfort for HVAC control.',
    signalRange: '-40 to 80 C',
    simulatedPin: 'A0'
  },
  humidity: {
    component: 'DHT22 humidity sensor',
    unit: 'percent relative humidity',
    purpose: 'Provides indoor-air comfort context.',
    signalRange: '0 to 100 percent',
    simulatedPin: 'A1'
  },
  occupancy: {
    component: 'PIR occupancy sensor',
    unit: 'people count',
    purpose: 'Estimates whether HVAC action should prioritise occupied rooms.',
    signalRange: '0 to 20 occupants',
    simulatedPin: 'D2'
  },
  co2: {
    component: 'NDIR CO2 sensor',
    unit: 'parts per million',
    purpose: 'Detects poor ventilation and supports air-quality decisions.',
    signalRange: '400 to 5000 ppm',
    simulatedPin: 'I2C'
  },
  actuator: {
    component: 'HVAC damper and fan actuator',
    unit: 'mode command',
    purpose: 'Receives cool, ventilate or idle commands from the controller.',
    signalRange: 'cool, ventilate, idle',
    simulatedPin: 'D5'
  }
};

export function buildDeviceManifest({ buildingId, deviceCount, zoneCount }) {
  return Array.from({ length: deviceCount }, (_, index) => {
    const sensorNumber = index + 1;
    const zoneNumber = (index % zoneCount) + 1;
    const floorNumber = Math.floor((zoneNumber - 1) / 4) + 1;

    return {
      buildingId,
      deviceId: `sensor-${String(sensorNumber).padStart(3, '0')}`,
      controllerId: `edge-node-${String(zoneNumber).padStart(2, '0')}`,
      zoneId: `zone-${zoneNumber}`,
      floor: floorNumber,
      room: `F${floorNumber}-R${String(zoneNumber).padStart(2, '0')}`,
      board: 'Simulated ESP32 development board',
      communication: 'MQTT over Wi-Fi',
      telemetryTopic: `${buildingId}/zone-${zoneNumber}/sensor-${String(sensorNumber).padStart(3, '0')}/telemetry`,
      commandTopic: `${buildingId}/zone-${zoneNumber}/hvac/command`,
      sensors: ['temperature', 'humidity', 'occupancy', 'co2'],
      actuator: 'actuator',
      sampleIntervalSeconds: 1
    };
  });
}

export function buildHardwareCatalogue() {
  return Object.entries(sensorTypes).map(([id, spec]) => ({
    id,
    ...spec
  }));
}

export function buildWiringRows(manifest) {
  return manifest.flatMap((device) =>
    device.sensors.map((sensorId) => {
      const sensor = sensorTypes[sensorId];
      return {
        deviceId: device.deviceId,
        zoneId: device.zoneId,
        room: device.room,
        board: device.board,
        component: sensor.component,
        simulatedPin: sensor.simulatedPin,
        communication: device.communication,
        telemetryTopic: device.telemetryTopic
      };
    })
  );
}
