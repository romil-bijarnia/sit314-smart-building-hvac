import { EventEmitter } from 'node:events';

export class EventBus {
  #emitter = new EventEmitter();

  publish(type, payload) {
    const event = {
      id: crypto.randomUUID(),
      type,
      timestamp: new Date().toISOString(),
      payload
    };

    this.#emitter.emit(type, event);
    this.#emitter.emit('*', event);
    return event;
  }

  subscribe(type, handler) {
    this.#emitter.on(type, handler);
    return () => this.#emitter.off(type, handler);
  }
}
