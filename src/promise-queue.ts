import EventEmitter from 'events';

interface IPromiseQueueItem<T> {
    value?: T;
}

interface IPromiseQueueEvents<T> {
    ready: (value: T) => void;
    error: (err: Error) => void;
}

// eslint-disable-next-line @typescript-eslint/naming-convention
export interface PromiseQueue<T> {
    on<E extends keyof IPromiseQueueEvents<T>>(event: E, listener: IPromiseQueueEvents<T>[E]): this;
    once<E extends keyof IPromiseQueueEvents<T>>(event: E, listener: IPromiseQueueEvents<T>[E]): this;
    addListener<E extends keyof IPromiseQueueEvents<T>>(event: E, listener: IPromiseQueueEvents<T>[E]): this;
    prependListener<E extends keyof IPromiseQueueEvents<T>>(event: E, listener: IPromiseQueueEvents<T>[E]): this;
    prependOnceListener<E extends keyof IPromiseQueueEvents<T>>(event: E, listener: IPromiseQueueEvents<T>[E]): this;
    emit<E extends keyof IPromiseQueueEvents<T>>(event: E, ...params: Parameters<IPromiseQueueEvents<T>[E]>): boolean;
}

export class PromiseQueue<T> extends EventEmitter {
    private items = [] as IPromiseQueueItem<T>[];

    public push(p: Promise<T>): void {
        const item: IPromiseQueueItem<T> = {};
        this.items.push(item);
        p.then((value) => {
            item.value = value;
            if (item === this.items[0]) {
                while (this.items.length > 0) {
                    /* c8 ignore start */
                    if (this.items[0].value === undefined) {
                        break;
                    }
                    /* c8 ignore stop */
                    this.emit('ready', this.items[0].value);
                    this.items.shift();
                }
            }
        }).catch((err: Error) => {
            /* c8 ignore start */
            const id = this.items.indexOf(item);
            this.items.splice(id, 1);
            this.emit('error', err);
            /* c8 ignore stop */
        });
    }
}
