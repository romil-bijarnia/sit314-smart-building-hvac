import { createWriteStream } from 'node:fs';
import { mkdir } from 'node:fs/promises';
import { dirname } from 'node:path';

export class StorageService {
  constructor({ bus, outputPath, enabled }) {
    this.outputPath = outputPath;
    this.enabled = enabled;
    this.count = 0;
    this.stream = null;
    this.unsubscribe = null;

    if (enabled) {
      this.ready = this.open(bus);
    } else {
      this.ready = Promise.resolve();
    }
  }

  async open(bus) {
    await mkdir(dirname(this.outputPath), { recursive: true });
    this.stream = createWriteStream(this.outputPath, { flags: 'w' });
    this.unsubscribe = bus.subscribe('*', (event) => {
      this.count += 1;
      this.stream.write(`${JSON.stringify(event)}\n`);
    });
  }

  async close() {
    await this.ready;

    if (this.unsubscribe) {
      this.unsubscribe();
    }

    if (!this.stream) {
      return;
    }

    await new Promise((resolve, reject) => {
      this.stream.end(resolve);
      this.stream.on('error', reject);
    });
  }

  snapshot() {
    return {
      enabled: this.enabled,
      outputPath: this.enabled ? this.outputPath : null,
      storedEvents: this.count
    };
  }
}
