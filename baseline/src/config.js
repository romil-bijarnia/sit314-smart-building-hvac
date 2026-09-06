export const defaultConfig = {
  buildingId: 'building-a',
  deviceCount: 24,
  zoneCount: 4,
  durationSeconds: 20,
  intervalMs: 1000,
  targetTemperature: 22,
  outputPath: 'data/events.jsonl',
  summaryPath: 'data/latest-summary.json',
  storageEnabled: true
};

export function parseArgs(argv) {
  const config = { ...defaultConfig };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    const next = argv[index + 1];

    if (arg === '--devices' && next) {
      config.deviceCount = Number(next);
      index += 1;
    } else if (arg === '--zones' && next) {
      config.zoneCount = Number(next);
      index += 1;
    } else if (arg === '--duration' && next) {
      config.durationSeconds = Number(next);
      index += 1;
    } else if (arg === '--interval' && next) {
      config.intervalMs = Number(next);
      index += 1;
    } else if (arg === '--target-temperature' && next) {
      config.targetTemperature = Number(next);
      index += 1;
    } else if (arg === '--output' && next) {
      config.outputPath = next;
      index += 1;
    } else if (arg === '--summary' && next) {
      config.summaryPath = next;
      index += 1;
    } else if (arg === '--no-storage') {
      config.storageEnabled = false;
    }
  }

  validateConfig(config);
  return config;
}

function validateConfig(config) {
  const numericFields = [
    ['deviceCount', 1, 10000],
    ['zoneCount', 1, 1000],
    ['durationSeconds', 1, 3600],
    ['intervalMs', 10, 60000],
    ['targetTemperature', 5, 40]
  ];

  for (const [field, min, max] of numericFields) {
    const value = config[field];

    if (!Number.isFinite(value) || value < min || value > max) {
      throw new Error(`${field} must be a number between ${min} and ${max}.`);
    }
  }
}
