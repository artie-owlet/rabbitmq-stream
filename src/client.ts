import EventEmitter from 'events';

import { Connection, IConnectionOptions } from './connection';
import {
    Commands,
    MAX_CORRELATION_ID,
    RESPONSE_CODE_OK,
    RESPONSE_FLAG,
    supportedServerCommands,
} from './messages/constants';
import { Offset } from './messages/offset';
import { ClientCommand, ClientRequest, ClientResponse } from './messages/client-message';
import { parseMessageHeader, parseResponseHeader } from './messages/server-message';
import { DeclarePublisherRequest } from './messages/declare-publisher';
import { Publish, IPublishEntry } from './messages/publish';
import { PublishConfirm } from './messages/publish-confirm';
import { PublishError, IPublishingError } from './messages/publish-error';
import { QueryPublisherRequest, QueryPublisherResponse } from './messages/query-publisher';
import { DeletePublisherRequest } from './messages/delete-publisher';
import { SubscribeRequest } from './messages/subscribe';
import { Deliver, DeliverData } from './messages/deliver';
import { Credit, CreditResponse } from './messages/credit';
import { StoreOffset } from './messages/store-offset';
import { QueryOffsetRequest, QueryOffsetResponse } from './messages/query-offset';
import { UnsubscribeRequest } from './messages/unsubscribe';
import { CreateStreamRequest } from './messages/create-stream';
import { DeleteStreamRequest } from './messages/delete-stream';
import { MetadataRequest, MetadataResponse, IStreamMetadata } from './messages/metadata';
import { MetadataUpdate } from './messages/metadata-update';
import { PeerPropertiesRequest, PeerPropertiesResponse } from './messages/peer-properties';
import { SaslHandshakeRequest, SaslHandshakeResponse } from './messages/sasl-handshake';
import { PlainSaslAuthenticateRequest } from './messages/sasl-authenticate';
import { ServerTune, ClientTune } from './messages/tune';
import { OpenRequest, OpenResponse } from './messages/open';
import { CloseRequest, CloseResponse } from './messages/close';
import { RouteRequest, RouteResponse } from './messages/route';
import { PartitionsRequest, PartitionsResponse } from './messages/partitions';
import { ConsumerUpdateRequest, ConsumerUpdateResponse } from './messages/consumer-update';
import {
    CommandVersionsExchangeRequest,
    CommandVersionsExchangeResponse,
    ICommandVersion,
} from './messages/command-versions-exchange';
import { StreamStatsRequest, StreamStatsResponse } from './messages/stream-stats';
import { StreamError } from './stream-error';
import { toError } from './utils';

export interface IClientOptions extends IConnectionOptions {
    username: string;
    password: string;
    vhost: string;
    frameMax?: number;
    /**
     * Heartbeat interval in secs
     */
    heartbeat?: number;
    /**
     * Request timeout in secs
     */
    requestTimeout?: number;
    connectionName?: string;
    /**
     * Disable checksum verification for incoming messages
     */
    disableDeliverCrcCheck?: boolean;
}

interface IRequest {
    key: number;
    version: number;
    ts: number;
    resolve: (resp: Buffer) => void;
    reject: (err: Error) => void;
}

const DEFAULT_REQUEST_TIMEOUT = 10;

function assertOpIdRange(id: number): void {
    if (id < 0 || id > 255) {
        throw new RangeError(`id must be >= 0 and < 256: ${id}`);
    }
}

function assertRefLength(ref: string): void {
    if (ref.length > 256) {
        throw new RangeError(`reference length must be <= 256: ${ref}`);
    }
}

/**
 * {@link Client} events
 */
export interface IClientEvents {
    open: () => void;
    publishConfirm: (publisherId: number, msgIds: bigint[]) => void;
    publishError: (publisherId: number, errors: IPublishingError[]) => void;
    deliver: (subscriptionId: number, data: DeliverData) => void;
    creditError: (subscriptionId: number, code: number) => void;
    metadataUpdate: (stream: string, code: number) => void;
    consumerUpdate: (subscriptionId: number, active: boolean, response: (offset: Offset) => void) => void;
    close: (reason: string) => void;
    error: (err: Error) => void;
}

// eslint-disable-next-line @typescript-eslint/naming-convention
export interface Client {
    on<E extends keyof IClientEvents>(event: E, listener: IClientEvents[E]): this;
    once<E extends keyof IClientEvents>(event: E, listener: IClientEvents[E]): this;
    addListener<E extends keyof IClientEvents>(event: E, listener: IClientEvents[E]): this;
    prependListener<E extends keyof IClientEvents>(event: E, listener: IClientEvents[E]): this;
    prependOnceListener<E extends keyof IClientEvents>(event: E, listener: IClientEvents[E]): this;
    removeListener<E extends keyof IClientEvents>(event: E, listener: IClientEvents[E]): this;
    removeAllListeners<E extends keyof IClientEvents>(event: E): this;
    emit<E extends keyof IClientEvents>(event: E, ...params: Parameters<IClientEvents[E]>): boolean;
}

/**
 * Provides [low-level API](https://github.com/rabbitmq/rabbitmq-server/blob/main/deps/rabbitmq_stream/docs/PROTOCOL.adoc)
 */
export class Client extends EventEmitter {
    /**
     * Returns an established client
     * @param host
     * @param port
     * @param options
     * @returns
     */
    public static async createClient(host: string, port: number, options: IClientOptions): Promise<Client> {
        return new Promise((res, rej) => {
            let lastErr: Error | null = null;
            const onError = (err: Error) => lastErr = err;
            const cli = new Client(host, port, options);
            cli.on('error', onError);
            cli.once('open', () => {
                cli.removeListener('error', onError);
                res(cli);
            });
            cli.once('close', (reason) => rej(lastErr === null ? new Error(`Connection closed: ${reason}`) : lastErr ));
        });
    }

    public readonly frameMax: number;
    public readonly heartbeat: number;
    public readonly requestTimeoutMs: number;

    public serverProperties = new Map<string, string>();

    private conn: Connection;
    private corrIdCounter = 0;
    private requests = new Map<number, IRequest>();
    private reqTimer: NodeJS.Timeout;
    private tuneReceived = false;
    private tuneTimer: NodeJS.Timeout | null = null;
    private closeReason = '';

    public get advertisedHost(): string {
        return this.serverProperties.get('advertised_host') /* c8 ignore next */ ?? '';
    }

    public async declarePublisher(publisherId: number, reference: string, stream: string): Promise<void> {
        assertOpIdRange(publisherId);
        assertRefLength(reference);
        await this.sendRequest(new DeclarePublisherRequest(publisherId, reference, stream));
    }

    public publish(publisherId: number, entries: IPublishEntry[]): void {
        assertOpIdRange(publisherId);
        this.sendMessage(new Publish(publisherId, entries));
    }

    public async queryPublisherSequence(reference: string, stream: string): Promise<bigint> {
        assertRefLength(reference);
        const res = new QueryPublisherResponse(await this.sendRequest(new QueryPublisherRequest(reference, stream)));
        return res.seq;
    }

    public async deletePublisher(publisherId: number): Promise<void> {
        assertOpIdRange(publisherId);
        await this.sendRequest(new DeletePublisherRequest(publisherId));
    }

    public async subscribe(subscriptionId: number, stream: string, offset: Offset, credit: number,
        properties: Map<string, string>,
    ): Promise<void> {
        assertOpIdRange(subscriptionId);
        await this.sendRequest(new SubscribeRequest(subscriptionId, stream, offset, credit, properties));
    }

    public credit(subscriptionId: number, credit: number): void {
        assertOpIdRange(subscriptionId);
        this.sendMessage(new Credit(subscriptionId, credit));
    }

    public storeOffset(reference: string, stream: string, offsetValue: bigint): void {
        assertRefLength(reference);
        this.sendMessage(new StoreOffset(reference, stream, offsetValue));
    }

    public async queryOffset(reference: string, stream: string): Promise<bigint> {
        assertRefLength(reference);
        const res = new QueryOffsetResponse(await this.sendRequest(new QueryOffsetRequest(reference, stream)));
        return res.offsetValue;
    }

    public async unsubscribe(subscriptionId: number): Promise<void> {
        assertOpIdRange(subscriptionId);
        await this.sendRequest(new UnsubscribeRequest(subscriptionId));
    }

    public async createStream(stream: string, args: Map<string, string>): Promise<void> {
        await this.sendRequest(new CreateStreamRequest(stream, args));
    }

    public async deleteStream(stream: string): Promise<void> {
        await this.sendRequest(new DeleteStreamRequest(stream));
    }

    public async metadata(streams: string[]): Promise<Map<string, IStreamMetadata>> {
        const res = new MetadataResponse(await this.sendRequest(new MetadataRequest(streams)));
        return res.streamsMetadata;
    }

    public async route(routingKey: string, superStream: string): Promise<string[]> {
        const res = new RouteResponse(await this.sendRequest(new RouteRequest(routingKey, superStream)));
        return res.streams;
    }

    public async partitions(superStream: string): Promise<string[]> {
        const res = new PartitionsResponse(await this.sendRequest(new PartitionsRequest(superStream)));
        return res.streams;
    }

    public async exchangeCommandVersion(): Promise<ICommandVersion[]> {
        const res = new CommandVersionsExchangeResponse(
            await this.sendRequest(new CommandVersionsExchangeRequest()),
        );
        return res.commands;
    }

    public async streamStats(stream: string): Promise<Map<string, bigint>> {
        const res = new StreamStatsResponse(await this.sendRequest(new StreamStatsRequest(stream)));
        return res.stats;
    }

    public close(): void {
        this.conn.close();
    }

    private constructor(
        host: string,
        port: number,
        private options: IClientOptions,
    ) {
        super();
        this.frameMax = options.frameMax || 0;
        this.heartbeat = options.heartbeat || 0;
        this.requestTimeoutMs = (options.requestTimeout || DEFAULT_REQUEST_TIMEOUT) * 1000;
        this.reqTimer = setInterval(this.onRequestTimeout.bind(this), this.requestTimeoutMs / 10);

        this.conn = new Connection(host, port, options);
        this.conn.on('connect', this.onConnect.bind(this));
        this.conn.on('message', this.onMessage.bind(this));
        this.conn.on('close', this.onClose.bind(this));
        this.conn.on('error', (err: Error) => this.emit('error', err));
    }

    private async onConnect(): Promise<void> {
        try {
            await this.peerPropertiesExchange();
            await this.authenticate();
            if (!this.tuneReceived) {
                this.tuneTimer = setTimeout(() => {
                    this.emit('error', new Error('Tune timeout'));
                    this.close();
                }, this.requestTimeoutMs);
            }
        } catch (err) {
            this.emit('error', toError(err));
            this.close();
        }
    }

    private async peerPropertiesExchange(): Promise<void> {
        const props = new PeerPropertiesResponse(
            await this.sendRequest(new PeerPropertiesRequest(this.options.connectionName)));
        this.serverProperties = props.properties;
    }

    private async authenticate(): Promise<void> {
        const handshake = new SaslHandshakeResponse(await this.sendRequest(new SaslHandshakeRequest()));

        /* c8 ignore start */
        if (!handshake.mechanisms.includes('PLAIN')) {
            throw new Error(
                'Authentication failed: server does not support PLAIN authentication',
            );
        }
        /* c8 ignore stop */
        await this.sendRequest(new PlainSaslAuthenticateRequest(this.options.username, this.options.password));
    }

    private async sendRequest(req: ClientRequest): Promise<Buffer> {
        return new Promise((resolve, reject) => {
            /* c8 ignore start */
            if (this.corrIdCounter === MAX_CORRELATION_ID) {
                this.corrIdCounter = 0;
            }
            /* c8 ignore stop */
            const corrId = ++this.corrIdCounter;
            this.requests.set(corrId, {
                key: req.key,
                version: req.version,
                ts: Date.now(),
                resolve,
                reject,
            });
            this.conn.sendMessage(req.serialize(corrId));
        });
    }

    private sendMessage(msg: ClientCommand | ClientResponse): void {
        this.conn.sendMessage(msg.serialize());
    }

    private onMessage(msg: Buffer): void {
        const [key, version] = parseMessageHeader(msg);
        if ((key & RESPONSE_FLAG) !== 0 &&
            key !== Commands.CreditResponse &&
            key !== Commands.MetadataResponse) {
            this.onResponse(key & (0xFFFF ^ RESPONSE_FLAG), version, msg);
        } else if (key !== Commands.Heartbeat) {
            this.onCommand(key, version, msg);
        }
    }

    private onResponse(key: number, version: number, msg: Buffer): void {
        try {
            const [corrId, code] = parseResponseHeader(msg);
            const req = this.requests.get(corrId);
            if (req) {
                /* c8 ignore start */
                if (req.key !== key || req.version !== version) {
                    throw new Error('Response key or version mismatch');
                }
                /* c8 ignore stop */
                if (code === RESPONSE_CODE_OK) {
                    req.resolve(msg);
                } else {
                    req.reject(new StreamError(key, code));
                }
                this.requests.delete(corrId);
            /* c8 ignore start */
            } else {
                throw new Error(`Unexpected response for command ${Commands[key]}`);
            }
            /* c8 ignore stop */
        /* c8 ignore start */
        } catch (err) {
            this.emit('error', toError(err));
        }
        /* c8 ignore stop */
    }

    private onCommand(key: number, version: number, msg: Buffer): void {
        try {
            const supportedVersions = supportedServerCommands.get(key);
            if (!supportedVersions) {
                throw new Error(`Unknown server command, key=${key}`);
            }
            if (!supportedVersions.includes(version)) {
                throw new Error(`Unsupported version ${version} for command ${Commands[key]}`);
            }

            switch (key) {
                case Commands.PublishConfirm:
                    return this.onPublishConfirm(msg);
                case Commands.PublishError:
                    return this.onPublishError(msg);
                case Commands.Deliver:
                    return this.onDeliver(version, msg);
                case Commands.CreditResponse:
                    return this.onCreditResponse(msg);
                case Commands.MetadataResponse:
                    return this.onMetadataResponse(msg);
                case Commands.MetadataUpdate:
                    return this.onMetadataUpdate(msg);
                case Commands.Tune:
                    return this.onTune(msg);
                /* c8 ignore start */
                case Commands.Close:
                    return this.onServerClose(msg);
                /* c8 ignore stop */
                case Commands.ConsumerUpdate:
                    return this.onConsumerUpdate(msg);
            }
        } catch (err) {
            this.emit('error', toError(err));
        }
    }

    private onPublishConfirm(msg: Buffer): void {
        const conf = new PublishConfirm(msg);
        this.emit('publishConfirm', conf.publisherId, conf.msgIds);
    }

    private onPublishError(msg: Buffer): void {
        const err = new PublishError(msg);
        this.emit('publishError', err.publisherId, err.errors);
    }

    private onDeliver(version: number, msg: Buffer): void {
        const deliver = new Deliver(msg, version, this.options.disableDeliverCrcCheck || false);
        this.emit('deliver', deliver.subscriptionId, deliver.deliverData);
    }

    private onCreditResponse(msg: Buffer): void {
        const res = new CreditResponse(msg);
        if (res.code !== RESPONSE_CODE_OK) {
            this.emit('creditError', res.subscriptionId, res.code);
        }
    }

    private onMetadataResponse(msg: Buffer): void {
        const corrId = MetadataResponse.getCorrelationId(msg);
        const req = this.requests.get(corrId);
        if (req) {
            req.resolve(msg);
            this.requests.delete(corrId);
        /* c8 ignore start */
        } else {
            throw new Error('Unexpected response for command Metadata');
        }
        /* c8 ignore stop */
    }

    private onMetadataUpdate(msg: Buffer): void {
        const upd = new MetadataUpdate(msg);
        this.emit('metadataUpdate', upd.stream, upd.code);
    }

    private onTune(msg: Buffer): void {
        this.tuneReceived = true;
        if (this.tuneTimer !== null) {
            clearTimeout(this.tuneTimer);
            this.tuneTimer = null;
        }

        const serverTune = new ServerTune(msg);
        const frameMax = (this.frameMax === 0 || serverTune.frameMax === 0) ?
            Math.max(this.frameMax, serverTune.frameMax) : Math.min(this.frameMax, serverTune.frameMax);
        const heartbeat = (this.heartbeat === 0 || serverTune.heartbeat === 0) ?
            Math.max(this.heartbeat, serverTune.heartbeat) : Math.min(this.heartbeat, serverTune.heartbeat);
        this.conn.setFrameMax(frameMax);
        this.conn.setHeartbeat(heartbeat);
        this.sendMessage(new ClientTune(frameMax, heartbeat));

        void this.open();
    }

    private async open(): Promise<void> {
        try {
            const res = new OpenResponse(await this.sendRequest(new OpenRequest(this.options.vhost)));
            res.properties.forEach((value, key) => this.serverProperties.set(key, value));
            this.emit('open');
        } catch (err) {
            this.emit('error', toError(err));
            this.close();
        }
    }

    /* c8 ignore start */
    private onServerClose(msg: Buffer): void {
        const closeReq = new CloseRequest(msg);
        this.closeReason = closeReq.reason;
        this.sendMessage(new CloseResponse(closeReq.corrId));
        this.close();
    }
    /* c8 ignore stop */

    private onConsumerUpdate(msg: Buffer): void {
        const req = new ConsumerUpdateRequest(msg);
        this.emit('consumerUpdate', req.subscriptionId, req.active, (offset: Offset) => {
            this.sendMessage(new ConsumerUpdateResponse(req.corrId, offset));
        });
    }

    private onClose(): void {
        this.requests.forEach((req) => {
            req.reject(new Error('Connection closed'));
        });
        clearInterval(this.reqTimer);
        this.emit('close', this.closeReason);
    }

    private onRequestTimeout(): void {
        const now = Date.now();
        this.requests.forEach((req, id) => {
            if (now - req.ts > this.requestTimeoutMs) {
                req.reject(new Error('Request timeout'));
                this.requests.delete(id);
            }
        });
    }
}
