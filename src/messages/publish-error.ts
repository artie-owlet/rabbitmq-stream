import { ServerMessage } from './server-message';

export interface IPublishingError {
    id: bigint;
    code: number;
}

export class PublishError extends ServerMessage {
    public readonly publisherId: number;
    public readonly errors = [] as IPublishingError[];

    constructor(msg: Buffer) {
        super(msg);
        this.publisherId = this.readUInt8();
        const size = this.readArraySize();
        for (let i = 0; i < size; ++i) {
            const id = this.readUInt64();
            const code = this.readUInt16();
            this.errors.push({
                id,
                code,
            });
        }
    }
}
