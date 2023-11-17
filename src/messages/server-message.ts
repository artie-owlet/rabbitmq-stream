import { DataReader } from './data-reader';

export function parseMessageHeader(msg: Buffer): [number, number] {
    const reader = new DataReader(msg, 4);
    return [
        reader.readUInt16(),
        reader.readUInt16(),
    ];
}

export class ServerMessage extends DataReader {
    constructor(
        msg: Buffer,
    ) {
        super(msg, 8);
    }
}

export class ServerRequest extends ServerMessage {
    public readonly corrId: number;

    constructor(
        msg: Buffer,
    ) {
        super(msg);
        this.corrId = this.readUInt32();
    }
}

export function parseResponseHeader(msg: Buffer): [number, number] {
    const reader = new DataReader(msg, 8);
    return [
        reader.readUInt32(),
        reader.readUInt16(),
    ];
}

export class ServerResponse extends DataReader {
    constructor(
        msg: Buffer,
    ) {
        super(msg, 14);
    }
}
