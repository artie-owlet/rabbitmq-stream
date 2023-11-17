import EventEmitter from 'events';

export function promisifyEvent<T = unknown>(em: EventEmitter, event: string): Promise<T> {
    return new Promise((res) => {
        em.once(event, (...args: unknown[]) => {
            res(args[0] as T);
        });
    });
}

export function promisifyEventArgs<T extends unknown[] = unknown[]>(em: EventEmitter, event: string): Promise<T> {
    return new Promise((res) => {
        em.once(event, (...args: unknown[]) => {
            res(args as T);
        });
    });
}
