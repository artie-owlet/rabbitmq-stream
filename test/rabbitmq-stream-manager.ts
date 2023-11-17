import { Writable } from 'stream';
import Dockerode from 'dockerode';
import { promisifyEvent } from './promisify-event';

class BufferStream extends Writable {
    private _data = '';
    private chunks = [] as Buffer[];

    get data(): string {
        return this._data;
    }

    override _write(chunk: Buffer, _encoding: unknown, callback: (error?: Error | null) => void): void {
        this.chunks.push(chunk);
        callback();
    }

    override _writev(chunks: Array<{ chunk: Buffer }>, callback: (error?: Error | null) => void): void {
        chunks.forEach((item) => this.chunks.push(item.chunk));
        callback();
    }

    override _final(callback: (error?: Error | null) => void): void {
        this._data += Buffer.concat(this.chunks).toString();
        callback();
    }
}

type Keys<T> = (keyof T) & string;

interface IOpts {
    [opt: string]: string | number;
}

function optsToArray(opts: IOpts | undefined): string[] {
    if (!opts) {
        return [];
    }

    const arr = [] as string[];
    for (const opt in opts) {
        if (opt.length === 1) {
            arr.push(`-${opt}`, String(opts[opt]));
        } else {
            arr.push(`--${opt}`, String(opts[opt]));
        }
    }
    return arr;
}

export interface IConnectionInfo {
    auth_mechanism: string;
    client_properties: [string, string, string][];
    frame_max: number;
    heartbeat: number;
    port: number;
    ssl: boolean;
    subscriptions: number;
    user: string;
    vhost: string;
}

export interface IPublisherInfo {
    publisher_id: number;
    reference: string;
    stream: string;
}

export interface IConsumerInfo {
    active: boolean;
    credits: number;
    messages_consumed: number;
    offset: number;
    properties: {
        [key: string]: string;
    };
    stream: string;
    subscription_id: number;
}

export class RabbitmqStreamManager {

    constructor(
        private containerName: string,
    ) {
    }

    public async getStreamConnection(name: string): Promise<IConnectionInfo | undefined> {
        const infos = await this.queryStreams<IConnectionInfo>('list_stream_connections', [
            'auth_mechanism', 'client_properties', 'frame_max', 'heartbeat', 'port', 'ssl',
            'subscriptions', 'user', 'vhost']);
        return infos.find(
            (info) => info.client_properties.findIndex(
                (prop) => prop[0] === 'connection_name' && prop[2] === name) >= 0);
    }

    public async getPublisher(ref: string, stream: string, vhost: string): Promise<IPublisherInfo | undefined> {
        const infos = await this.queryStreams<IPublisherInfo>('list_stream_publishers', [
            'publisher_id', 'reference', 'stream',
        ], { vhost });
        return infos.find((info) => info.stream === stream && info.reference === ref);
    }

    public async getConsumer(subId: number, stream: string, vhost: string): Promise<IConsumerInfo | undefined> {
        const infos = await this.queryStreams<IConsumerInfo>('list_stream_consumers', [
            'active', 'credits', 'messages_consumed', 'offset', 'properties', 'stream', 'subscription_id',
        ], { vhost });
        return infos.find((info) => info.stream === stream && info.subscription_id === subId);
    }

    public addReplica(stream: string, node: string, vhost: string): Promise<void> {
        return this.execStreams(['add_replica', stream, node], { vhost });
    }

    public deleteReplica(stream: string, node: string, vhost: string): Promise<void> {
        return this.execStreams(['delete_replica', stream, node], { vhost });
    }

    public createSuperStream(superStream: string, vhost: string): Promise<void> {
        return this.execStreams(['add_super_stream', superStream], { vhost });
    }

    private async queryStreams<R>(subCommand: string, args: Keys<R>[], opts?: IOpts): Promise<R[]> {
        const [out, err] = await this.execCommand(['rabbitmq-streams', '--formatter', 'json',
            subCommand, ...optsToArray(opts), ...args]);
        if (err) {
            throw new Error(err);
        }
        return JSON.parse(out) as R[];
    }

    private async execStreams(subCommand: string[], opts?: IOpts): Promise<void> {
        const [, err] = await this.execCommand(['rabbitmq-streams', ...subCommand, ...optsToArray(opts)]);
        if (err) {
            throw new Error(err);
        }
    }

    private async execCommand(Cmd: string[]): Promise<[string, string]> {
        const dock = new Dockerode();
        const container = dock.getContainer(this.containerName);
        const exec = await container.exec({
            Cmd,
            AttachStdout: true,
            AttachStderr: true,
        });
        const stream = await exec.start({});
        const stdout = new BufferStream();
        const stderr = new BufferStream();
        dock.modem.demuxStream(stream, stdout, stderr);
        await promisifyEvent(stream, 'end');
        stdout.end();
        stderr.end();
        return [stdout.data, stderr.data];
    }
}
