import { ServerMessage } from './server-message';

export class MetadataUpdate extends ServerMessage {
    public readonly code: number;
    public readonly stream: string;

    constructor(msg: Buffer) {
        super(msg);
        this.code = this.readUInt16();
        this.stream = this.readString();
    }
}
