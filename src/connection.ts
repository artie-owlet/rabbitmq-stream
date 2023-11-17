import EventEmitter from 'events';
import { connect, Socket } from 'net';
import { connect as tlsConnect, ConnectionOptions as TlsConnectionOptions, TLSSocket } from 'tls';

import { Commands } from './messages/constants';
import { ClientCommand } from './messages/client-message';

export interface IConnectionOptions {
    connectTimeoutMs?: number;
    noDelay?: boolean;
    /**
     * Specify to connect over TLS
     *
     * See [tls.connect() options](https://nodejs.org/dist/latest-v18.x/docs/api/tls.html#tlsconnectoptions-callback)
     */
    tls?: TlsConnectionOptions;
}

interface IConnectionEvents {
    connect: () => void;
    message: (msg: Buffer) => void;
    close: () => void;
    error: (err: Error) => void;
}

const hbMsg = new ClientCommand(Commands.Heartbeat, 1).serialize();

// eslint-disable-next-line @typescript-eslint/naming-convention
export interface Connection {
    on<E extends keyof IConnectionEvents>(event: E, listener: IConnectionEvents[E]): this;
    once<E extends keyof IConnectionEvents>(event: E, listener: IConnectionEvents[E]): this;
    addListener<E extends keyof IConnectionEvents>(event: E, listener: IConnectionEvents[E]): this;
    prependListener<E extends keyof IConnectionEvents>(event: E, listener: IConnectionEvents[E]): this;
    prependOnceListener<E extends keyof IConnectionEvents>(event: E, listener: IConnectionEvents[E]): this;
    emit<E extends keyof IConnectionEvents>(event: E, ...params: Parameters<IConnectionEvents[E]>): boolean;
}

export class Connection extends EventEmitter {
    private sock: Socket | TLSSocket;
    private connTimer?: NodeJS.Timeout;
    private frameMax = 0;
    private heartbeatTimer: NodeJS.Timeout | null = null;
    private dataReceived = false;
    private recvBuf: Buffer | null = null;

    constructor(
        host: string,
        port: number,
        options: IConnectionOptions,
    ) {
        super();

        if (options.tls === undefined) {
            this.sock = connect(port, host, this.onConnect.bind(this));
        } else {
            this.sock = tlsConnect(port, host, options.tls, this.onConnect.bind(this));
        }

        this.sock.on('data', this.onData.bind(this));
        this.sock.on('close', this.onClose.bind(this));
        this.sock.on('error', (err: Error) => this.emit('error', err));

        if (options.connectTimeoutMs !== undefined && options.connectTimeoutMs > 0) {
            this.connTimer = setTimeout(() => {
                this.sock.destroy(new Error('Connect timeout'));
            }, options.connectTimeoutMs);
        }

        if (options.noDelay === undefined) {
            this.sock.setNoDelay(true);
        } else {
            this.sock.setNoDelay(options.noDelay);
        }
    }

    public setFrameMax(frameMax: number): void {
        this.frameMax = frameMax;
    }

    public setHeartbeat(heartbeat: number): void {
        this.stopHeartbeat();
        if (heartbeat > 0) {
            this.heartbeatTimer = setInterval(this.onHeartbeatTimeout.bind(this), heartbeat * 1000);
        }
    }

    public sendMessage(msg: Buffer): void {
        /* c8 ignore start */
        if (this.frameMax > 0 && msg.length > this.frameMax) {
            throw new Error('Frame too large');
        }
        /* c8 ignore stop */
        if (this.sock.writable) {
            this.sock.write(msg);
        }
    }

    public close(): void {
        if (!this.sock.closed) {
            this.sock.end();
        }
        this.stopHeartbeat();
    }

    private onConnect() {
        if (this.connTimer !== undefined) {
            clearTimeout(this.connTimer);
        }
        this.dataReceived = true;
        this.emit('connect');
    }

    private onData(data: Buffer): void {
        this.dataReceived = true;

        if (this.recvBuf !== null) {
            this.recvBuf = Buffer.concat([this.recvBuf, data]);
        } else {
            this.recvBuf = data;
        }

        let offset = 0;
        while (offset < this.recvBuf.length) {
            if (offset + 4 > this.recvBuf.length) {
                break;
            }
            const msgSize = this.recvBuf.readUInt32BE(offset);
            if (offset + 4 + msgSize > this.recvBuf.length) {
                break;
            }
            this.emit('message', this.recvBuf.subarray(offset, offset + 4 + msgSize));
            offset += 4 + msgSize;
        }

        if (offset === this.recvBuf.length) {
            this.recvBuf = null;
        } else {
            this.recvBuf = this.recvBuf.subarray(offset);
        }
    }

    private onHeartbeatTimeout(): void {
        if (!this.dataReceived) {
            this.emit('error', new Error('Heartbeat timeout'));
            this.close();
            return;
        }
        this.dataReceived = false;
        this.sock.write(hbMsg);
    }

    private stopHeartbeat(): void {
        if (this.heartbeatTimer) {
            clearInterval(this.heartbeatTimer);
            this.heartbeatTimer = null;
        }
    }

    private onClose(): void {
        this.stopHeartbeat();
        this.emit('close');
    }
}
