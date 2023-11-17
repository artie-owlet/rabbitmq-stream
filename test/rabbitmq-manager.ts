import axios from 'axios';

interface IArguments {
    [key: string]: string | number;
}

interface IStreamInfo {
    arguments: IArguments;
    type: string;
}

export class RabbitmqManager {
    private client = axios.create({
        baseURL: 'http://localhost:15672/api/',
        auth: {
            username: 'guest',
            password: 'guest',
        },
        headers: {
            'Content-Type': 'application/json',
        },
        maxContentLength: 100000,
        maxBodyLength: 100000,
    });

    public async addVhost(vhost: string): Promise<void> {
        const vhostEncoded = encodeURIComponent(vhost);
        await this.client.put(`vhosts/${vhostEncoded}`);
        await this.client.put(`permissions/${vhostEncoded}/guest`, {
            configure: '.*',
            write: '.*',
            read: '.*',
        });
    }

    public async deleteVhost(vhost: string): Promise<void> {
        const vhostEncoded = encodeURIComponent(vhost);
        await this.client.delete(`vhosts/${vhostEncoded}`, {
            validateStatus: (status) => (status >= 200 && status < 300) || status === 404,
        });
    }

    public async getStream(stream: string, vhost: string): Promise<IStreamInfo | undefined> {
        const streamEncoded = encodeURIComponent(stream);
        const vhostEncoded = encodeURIComponent(vhost);
        const { data, status } = await this.client.get<IStreamInfo>(`queues/${vhostEncoded}/${streamEncoded}`, {
            validateStatus: (status) => (status >= 200 && status < 300) || status === 404,
        });
        if (status === 404) {
            return undefined;
        }
        return data;
    }

    public async createStream(stream: string, vhost: string): Promise<void> {
        const streamEncoded = encodeURIComponent(stream);
        const vhostEncoded = encodeURIComponent(vhost);
        await this.client.put<unknown>(`queues/${vhostEncoded}/${streamEncoded}`, {
            arguments: {
                'x-queue-type': 'stream',
            },
            durable: true,
        });
    }

    public async publish(payload: string, stream: string, vhost: string): Promise<void> {
        const vhostEncoded = encodeURIComponent(vhost);
        await this.client.post(`exchanges/${vhostEncoded}/amq.default/publish`, {
            properties: {},
            routing_key: stream,
            payload,
            payload_encoding: 'string',
        });
    }
}
