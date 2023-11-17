import { RESPONSE_FLAG } from './constants';
import { DataWriter } from './data-writer';

enum Types {
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Bytes,
    String,
    RawData,
}

type IntField = [Types.Int8 | Types.Int16 | Types.Int32 | Types.UInt8 | Types.UInt16 | Types.UInt32, number];
type BigIntField = [Types.Int64 | Types.UInt64, bigint];
type BytesField = [Types.Bytes, Buffer | null];
type StringField = [Types.String, string | null];
type RawDataField = [Types.RawData, Buffer];

class ClientMessage {
    private size = 0;
    private fields = [] as (IntField | BigIntField | BytesField | StringField | RawDataField)[];

    constructor(
        public readonly key: number,
        public readonly version: number,
    ) {
    }

    protected writeHeader(): void {
        this.fields.length = 0;
        this.writeUInt16(this.key);
        this.writeUInt16(this.version);
    }

    protected writeInt8(n: number): void {
        this.size += 1;
        this.fields.push([Types.Int8, n]);
    }

    protected writeInt16(n: number): void {
        this.size += 2;
        this.fields.push([Types.Int16, n]);
    }

    protected writeInt32(n: number): void {
        this.size += 4;
        this.fields.push([Types.Int32, n]);
    }

    protected writeInt64(n: bigint): void {
        this.size += 8;
        this.fields.push([Types.Int64, n]);
    }

    protected writeUInt8(n: number): void {
        this.size += 1;
        this.fields.push([Types.UInt8, n]);
    }

    protected writeUInt16(n: number): void {
        this.size += 2;
        this.fields.push([Types.UInt16, n]);
    }

    protected writeUInt32(n: number): void {
        this.size += 4;
        this.fields.push([Types.UInt32, n]);
    }

    protected writeUInt64(n: bigint): void {
        this.size += 8;
        this.fields.push([Types.UInt64, n]);
    }

    protected writeBytes(bytes: Buffer | null): void {
        this.size += 4 + (bytes !== null ? bytes.length /* c8 ignore next */ : 0);
        this.fields.push([Types.Bytes, bytes]);
    }

    protected writeString(str: string | null): void {
        this.size += 2 + (str !== null ? Buffer.byteLength(str) /* c8 ignore next */ : 0);
        this.fields.push([Types.String, str]);
    }

    protected writeArraySize(n: number): void {
        this.writeInt32(n);
    }

    protected writeRawData(bytes: Buffer): void {
        this.size += bytes.length;
        this.fields.push([Types.RawData, bytes]);
    }

    protected serializeImpl(): Buffer {
        const msg = Buffer.allocUnsafe(this.size + 4);
        const writer = new DataWriter(msg);
        writer.writeUInt32(this.size);
        this.fields.forEach(([t, val]) => {
            switch (t) {
                case Types.Int8:
                    return writer.writeInt8(val);
                case Types.Int16:
                    return writer.writeInt16(val);
                case Types.Int32:
                    return writer.writeInt32(val);
                case Types.Int64:
                    return writer.writeInt64(val);
                case Types.UInt8:
                    return writer.writeUInt8(val);
                case Types.UInt16:
                    return writer.writeUInt16(val);
                case Types.UInt32:
                    return writer.writeUInt32(val);
                case Types.UInt64:
                    return writer.writeUInt64(val);
                case Types.Bytes:
                    return writer.writeBytes(val);
                case Types.String:
                    return writer.writeString(val);
                case Types.RawData:
                    return writer.writeRawData(val);
            }
        });
        return msg;
    }
}

export class ClientCommand extends ClientMessage {
    public serialize(): Buffer {
        this.build();
        return this.serializeImpl();
    }

    protected build(): void {
        this.writeHeader();
    }
}

export class ClientRequest extends ClientMessage {
    public serialize(corrId: number): Buffer {
        this.build(corrId);
        return this.serializeImpl();
    }

    protected build(corrId: number): void {
        this.writeHeader();
        this.writeUInt32(corrId);
    }
}

export class ClientResponse extends ClientMessage {
    constructor(
        key: number,
        version: number,
        private corrId: number,
        private code: number,
    ) {
        super(key | RESPONSE_FLAG, version);
    }

    public serialize(): Buffer {
        this.build();
        return this.serializeImpl();
    }

    protected build(): void {
        this.writeHeader();
        this.writeUInt32(this.corrId);
        this.writeUInt16(this.code);
    }
}
