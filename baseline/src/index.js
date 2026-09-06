import { parseArgs } from './config.js';
import { runSimulation } from './simulation.js';

try {
  const config = parseArgs(process.argv.slice(2));
  const summary = await runSimulation(config);
  console.log(JSON.stringify(summary, null, 2));
} catch (error) {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
}
