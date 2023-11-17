import EventEmitter from 'events';

import { Client, IClientOptions } from './client';
import { ClientWrapper } from './client-wrapper';

function rand(ceil: number): number {
    return Math.floor(Math.random() * ceil);
}

export interface IClientPoolEvents {
    error: (err: Error) => void;
}

// eslint-disable-next-line @typescript-eslint/naming-convention
export interface ClientPool {
    on<E extends keyof IClientPoolEvents>(event: E, listener: IClientPoolEvents[E]): this;
    once<E extends keyof IClientPoolEvents>(event: E, listener: IClientPoolEvents[E]): this;
    addListener<E extends keyof IClientPoolEvents>(event: E, listener: IClientPoolEvents[E]): this;
    prependListener<E extends keyof IClientPoolEvents>(event: E, listener: IClientPoolEvents[E]): this;
    prependOnceListener<E extends keyof IClientPoolEvents>(event: E, listener: IClientPoolEvents[E]): this;
    removeListener<E extends keyof IClientPoolEvents>(event: E, listener: IClientPoolEvents[E]): this;
    removeAllListeners<E extends keyof IClientPoolEvents>(event: E): this;
    emit<E extends keyof IClientPoolEvents>(event: E, ...params: Parameters<IClientPoolEvents[E]>): boolean;
}

export abstract class ClientPool extends EventEmitter {
    protected clientWrappers = new Set<ClientWrapper>();

    constructor() {
        super();
    }

    public async acquireClient(nodes?: string[]): Promise<ClientWrapper> {
        if (nodes && nodes.length === 0) {
            throw new Error('List of nodes is empty');
        }
        if (this.clientWrappers.size > 0) {
            const wrappers = Array.from(this.clientWrappers.values())
                .filter(w => !nodes || nodes.includes(w.client.advertisedHost));
            if (wrappers.length > 0) {
                const wrapper = wrappers[rand(wrappers.length)];
                wrapper.ref();
                return wrapper;
            }
        }

        return this.createClient(nodes);
    }

    public releaseClient(wrapper: ClientWrapper): void {
        if (wrapper.unref() === 0) {
            this.clientWrappers.delete(wrapper);
            wrapper.client.close();
        }
    }

    protected abstract createClient(nodes?: string[]): Promise<ClientWrapper>;

    protected addClient(client: Client): ClientWrapper {
        const existingWrapper = this.findClient(client.advertisedHost);
        if (existingWrapper) {
            client.close();
            return existingWrapper;
        }

        const wrapper = new ClientWrapper(client);
        client.on('error', err => this.emit('error', new Error(`${client.advertisedHost}: ${err.message}`)));
        client.on('close', () => this.clientWrappers.delete(wrapper));
        this.clientWrappers.add(wrapper);
        return wrapper;
    }

    private findClient(node: string): ClientWrapper | undefined {
        return Array.from(this.clientWrappers.values()).find(w => w.client.advertisedHost === node);
    }
}

export interface IEndpoint {
    host: string;
    port: number;
}

export class Cluster extends ClientPool {
    constructor(
        private conf: Map<string, IEndpoint>,
        private clientOpts: IClientOptions,
    ) {
        super();
    }

    public addNode(node: string, endpoint: IEndpoint): void {
        this.conf.set(node, endpoint);
    }

    public removeNode(node: string): void {
        this.conf.delete(node);
    }

    protected override async createClient(nodes?: string[]): Promise<ClientWrapper> {
        const eligibleNodes = Array.from(this.conf.entries())
            .filter(([node, _]) => !nodes || nodes.includes(node));
        if (eligibleNodes.length === 0) {
            throw new Error('No eligible nodes');
        }

        const [node, { host, port }] = eligibleNodes[rand(eligibleNodes.length)];
        const client = await Client.createClient(host, port, this.clientOpts);
        if (client.advertisedHost !== node) {
            client.close();
            throw new Error(`Wrong config: node ${node} has actual advertised host ${client.advertisedHost}`);
        }

        return this.addClient(client);
    }
}

export class LoadBalancer extends ClientPool {
    constructor(
        private host: string,
        private port: number,
        private maxAttempts: number,
        private clientOpts: IClientOptions,
    ) {
        super();
        if (this.maxAttempts === 0) {
            this.maxAttempts = Infinity;
        }
    }

    protected override async createClient(nodes?: string[]): Promise<ClientWrapper> {
        for (let i = 0; i < this.maxAttempts; ++i) {
            const wrapper = this.addClient(await Client.createClient(this.host, this.port, this.clientOpts));
            if (!nodes || nodes.includes(wrapper.client.advertisedHost)) {
                return wrapper;
            }
        }
        throw new Error(`Cannot create client for nodes ${(nodes ?? []).join(', ')}: maxAttempts exceeded`);
    }
}
