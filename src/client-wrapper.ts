import EventEmitter from 'events';

import { Client } from './client';
import { Commands } from './messages/constants';
import { DeliverData } from './messages/deliver';
import { Offset } from './messages/offset';
import { IPublishingError } from './messages/publish-error';
import { MultiMap } from './multi-map';
import { StreamError } from './stream-error';

interface IStreamOperatorEventListeners {
    onStreamUnavailable: () => void;
    onClose: () => void;
}

export interface IPublisherEventListeners extends IStreamOperatorEventListeners {
    onPublishConfirm: (msgIds: bigint[]) => void;
    onPublishError: (errors: IPublishingError[]) => void;
}

export interface IConsumerEventListeners extends IStreamOperatorEventListeners {
    onDeliver: (data: DeliverData) => void;
    onConsumerUpdate?: (active: boolean, response: (offset: Offset) => void) => void;
}

interface IStreamOperator {
    id: number;
    stream: string;
}

interface IPublisher extends IStreamOperator {
    listeners: IPublisherEventListeners;
}

interface IConsumer extends IStreamOperator {
    listeners: IConsumerEventListeners;
}

export interface IClientWrapperEvents {
    error: (err: Error) => void;
}

// eslint-disable-next-line @typescript-eslint/naming-convention
export interface ClientWrapper {
    on<E extends keyof IClientWrapperEvents>(event: E, listener: IClientWrapperEvents[E]): this;
    once<E extends keyof IClientWrapperEvents>(event: E, listener: IClientWrapperEvents[E]): this;
    addListener<E extends keyof IClientWrapperEvents>(event: E, listener: IClientWrapperEvents[E]): this;
    prependListener<E extends keyof IClientWrapperEvents>(event: E, listener: IClientWrapperEvents[E]): this;
    prependOnceListener<E extends keyof IClientWrapperEvents>(event: E, listener: IClientWrapperEvents[E]): this;
    removeListener<E extends keyof IClientWrapperEvents>(event: E, listener: IClientWrapperEvents[E]): this;
    removeAllListeners<E extends keyof IClientWrapperEvents>(event: E): this;
    emit<E extends keyof IClientWrapperEvents>(event: E, ...params: Parameters<IClientWrapperEvents[E]>): boolean;
}

export class ClientWrapper extends EventEmitter {
    private refCount = 1;
    private publishers = new Array<IPublisher | null>(256).fill(null);
    private consumers = new Array<IConsumer | null>(256).fill(null);
    private streamPublishers = new MultiMap<string, IPublisher>();
    private streamConsumers = new MultiMap<string, IConsumer>();

    constructor(
        public readonly client: Client,
    ) {
        super();
        client.on('publishConfirm', this.onPublishConfirm.bind(this));
        client.on('publishError', this.onPublishError.bind(this));
        client.on('deliver', this.onDeliver.bind(this));
        client.on('creditError', this.onCreditError.bind(this));
        client.on('metadataUpdate', this.onMetadataUpdate.bind(this));
        client.on('consumerUpdate', this.onConsumerUpdate.bind(this));
        client.on('close', this.onClose.bind(this));
    }

    public ref(): number {
        return ++this.refCount;
    }

    public unref(): number {
        return --this.refCount;
    }

    public acquirePublisher(stream: string, listeners: IPublisherEventListeners): number {
        const id = this.publishers.indexOf(null);
        if (id < 0) {
            throw new Error('Max number of publishers exceeded');
        }
        const publisher: IPublisher = { id, stream, listeners };
        this.publishers[id] = publisher;
        this.streamPublishers.add(stream, publisher);
        return id;
    }

    public releasePublisher(publisherId: number): void {
        const publisher = this.publishers[publisherId];
        if (!publisher) {
            throw new Error(`Publisher ${publisherId} does not exist`);
        }
        this.publishers[publisherId] = null;
        this.streamPublishers.delete(publisher.stream, publisher);
    }

    public acquireConsumer(stream: string, listeners: IConsumerEventListeners): number {
        const id = this.consumers.indexOf(null);
        if (id < 0) {
            throw new Error('Max number of consumers exceeded');
        }
        const consumer = { id, stream, listeners };
        this.consumers[id] = consumer;
        this.streamConsumers.add(stream, consumer);
        return id;
    }

    public releaseConsumer(subscriptionId: number): void {
        const consumer = this.consumers[subscriptionId];
        if (!consumer) {
            throw new Error(`Consumer ${subscriptionId} does not exist`);
        }
        this.consumers[subscriptionId] = null;
        this.streamConsumers.delete(consumer.stream, consumer);
    }

    private onPublishConfirm(publisherId: number, msgIds: bigint[]): void {
        const publisher = this.publishers[publisherId];
        if (publisher) {
            publisher.listeners.onPublishConfirm(msgIds);
        }
    }

    private onPublishError(publisherId: number, errors: IPublishingError[]): void {
        const publisher = this.publishers[publisherId];
        if (publisher) {
            publisher.listeners.onPublishError(errors);
        }
    }

    private onDeliver(subscriptionId: number, data: DeliverData): void {
        const consumer = this.consumers[subscriptionId];
        if (consumer) {
            consumer.listeners.onDeliver(data);
        }
    }

    private onCreditError(_subscriptionId: number, code: number): void {
        this.emit('error', new StreamError(Commands.Credit, code));
    }

    private onMetadataUpdate(stream: string, code: number): void {
        if (code !== 0x02) {
            this.emit('error', new Error(`Unsupported metadata update code ${code}`));
        }

        const publishers = this.streamPublishers.getAll(stream);
        if (publishers) {
            publishers.forEach((publisher) => {
                publisher.listeners.onStreamUnavailable();
                this.releasePublisher(publisher.id);
            });
        }

        const consumers = this.streamConsumers.getAll(stream);
        if (consumers) {
            consumers.forEach((consumer) => {
                consumer.listeners.onStreamUnavailable();
                this.releaseConsumer(consumer.id);
            });
        }
    }

    private onConsumerUpdate(subscriptionId: number, active: boolean, response: (offset: Offset) => void): void {
        const consumer = this.consumers[subscriptionId];
        if (consumer && consumer.listeners.onConsumerUpdate) {
            consumer.listeners.onConsumerUpdate(active, response);
        }
    }

    private onClose(): void {
        this.publishers.forEach(publisher => publisher?.listeners.onClose());
        this.consumers.forEach(consumer => consumer?.listeners.onClose());
    }
}
