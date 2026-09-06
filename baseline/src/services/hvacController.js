export class HvacController {
  constructor({ bus, targetTemperature }) {
    this.bus = bus;
    this.targetTemperature = targetTemperature;
    this.commands = [];
    this.unsubscribe = bus.subscribe('zone.metrics.updated', (event) => this.handleMetrics(event));
  }

  handleMetrics(event) {
    const metrics = event.payload;
    const command = decideCommand(metrics, this.targetTemperature);
    this.commands.push(command);

    this.bus.publish('actuator.command.created', command);
  }

  snapshot() {
    const byMode = this.commands.reduce((summary, command) => {
      summary[command.mode] = (summary[command.mode] ?? 0) + 1;
      return summary;
    }, {});

    return {
      totalCommands: this.commands.length,
      byMode,
      latestCommands: this.commands.slice(-10)
    };
  }

  close() {
    this.unsubscribe();
  }
}

function decideCommand(metrics, targetTemperature) {
  const tooHot = metrics.averageTemperatureC > targetTemperature + 1.5;
  const tooMuchCo2 = metrics.averageCo2Ppm > 900;
  const occupied = metrics.averageOccupancyCount >= 2;

  if (tooHot && occupied) {
    return {
      zoneId: metrics.zoneId,
      mode: 'cool',
      fanSpeed: tooMuchCo2 ? 'high' : 'medium',
      reason: 'temperature above target while occupied'
    };
  }

  if (tooMuchCo2 && occupied) {
    return {
      zoneId: metrics.zoneId,
      mode: 'ventilate',
      fanSpeed: 'high',
      reason: 'co2 above comfort threshold'
    };
  }

  return {
    zoneId: metrics.zoneId,
    mode: 'idle',
    fanSpeed: 'low',
    reason: 'zone is within comfort range'
  };
}
