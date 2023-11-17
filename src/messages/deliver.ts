import { ServerMessage } from './server-message';
import { CompressionTypes } from './constants';
import { crc32 } from './crc32';
import { DataReader } from './data-reader';
import { decodeGzip } from './gzip';

export type Decoder = (input: Buffer) => Promise<Buffer>;

const decoders = new Map<CompressionTypes, Decoder>();

/**
 * Set the decoding function to decompress batched messages
 * @param compressionType
 * @param decode
 */
export function installDecoder(compressionType: CompressionTypes, decode: Decoder): void {
    decoders.set(compressionType, decode);
}

installDecoder(CompressionTypes.Gzip, decodeGzip);

/* c8 ignore start */
class DeliverParseError extends Error {
    constructor(reason: string) {
        super(`Failed to parse Deliver: ${reason}`);
    }
}
/* c8 ignore stop */

export class DeliverData {
    constructor(
        public readonly committedChunkId: number,
        private numEntries: number,
        private numRecords: number,
        public readonly timestamp: bigint,
        public readonly offsetValue: bigint,
        private data: Buffer,
    ) {
    }

    public async readMessages(): Promise<Buffer[]> {
        const records = new Array<Buffer>(this.numRecords);
        const reader = new DataReader(this.data, 0);
        const uncmpJobs = [] as Promise<void>[];
        let recId = 0;
        for (let i = 0; i < this.numEntries; ++i) {
            const entryType = reader.readUInt8();
            if ((entryType & 0x80) === 0) {
                reader.unshift(1);
                records[recId] = reader.readBytes();
                ++recId;
            } else {
                const cmpType = (entryType & 0x70) >> 4;
                const recordsInBatch = reader.readUInt16();
                reader.shift(4); // Uncompressed Length
                const data = reader.readBytes();
                /* c8 ignore start */
                if (cmpType === CompressionTypes.None) {
                    for (let i = 0; i < recordsInBatch; ++i) {
                        records[recId + i] = reader.readBytes();
                    }
                /* c8 ignore stop */
                } else {
                    const decode = decoders.get(cmpType);
                    /* c8 ignore start */
                    if (!decode) {
                        throw new DeliverParseError(`compression type ${cmpType} not supported`);
                    }
                    /* c8 ignore stop */
                    uncmpJobs.push((async () => {
                        const start = recId;
                        const uncmpData = await decode(data);
                        const entryReader = new DataReader(uncmpData, 0);
                        for (let i = 0; i < recordsInBatch; ++i) {
                            records[start + i] = entryReader.readBytes();
                        }
                    })());
                }
                recId += recordsInBatch;
            }
        }
        if (uncmpJobs.length > 0) {
            await Promise.all(uncmpJobs);
        }
        return records;
    }
}

export class Deliver extends ServerMessage {
    public readonly subscriptionId: number;
    public readonly deliverData: DeliverData;

    constructor(
        private msg: Buffer,
        version: number,
        disableCrcCheck: boolean,
    ) {
        super(msg);
        this.subscriptionId = this.readUInt8();
        const committedChunkId = version === 1 ? 0 /* c8 ignore next */ : this.readUInt32();
        this.shift(1); // MagicVersion
        const chunkType = this.readUInt8();
        /* c8 ignore start */
        if (chunkType !== 0) {
            throw new DeliverParseError(`invalid chunk type ${chunkType}`);
        }
        /* c8 ignore stop */
        const numEntries = this.readUInt16();
        const numRecords = this.readUInt32();
        const timestamp = this.readInt64();
        this.shift(8); // Epoch
        const offsetValue = this.readUInt64();
        const checksum = this.readUInt32();
        const dataLength = this.readUInt32();
        this.shift(8); // TrailerLength, Reserved
        const data = this.msg.subarray(this.getOffset(), this.getOffset() + dataLength);
        /* c8 ignore start */
        if (!disableCrcCheck) {
            if (crc32(data) !== checksum) {
                throw new Error('Failed to parse Deliver: wrong checksum');
            }
        }
        /* c8 ignore stop */
        this.deliverData = new DeliverData(committedChunkId, numEntries, numRecords, timestamp, offsetValue, data);
    }
}
