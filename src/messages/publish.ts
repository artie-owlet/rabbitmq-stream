import { gzip } from 'zlib';
import { promisify } from 'util';

import { Commands, CompressionTypes } from './constants';
import { ClientCommand } from './client-message';
import { DataWriter } from './data-writer';

const gzipAsync = promisify(gzip);

export interface IPublishEntry {
    id: bigint;
    payload: Buffer;
    batch?: boolean;
}

export class Publish extends ClientCommand {
    constructor(
        private publisherId: number,
        private entries: IPublishEntry[],
    ) {
        super(Commands.Publish, 1);
    }

    protected override build(): void {
        super.build();
        this.writeUInt8(this.publisherId);
        this.writeArraySize(this.entries.length);
        this.entries.forEach(({ id, payload, batch }) => {
            this.writeUInt64(id);
            if (batch) {
                this.writeRawData(payload);
            } else {
                this.writeBytes(payload);
            }
        });
    }
}

export async function createBatch(messages: Buffer[]): Promise<Buffer> {
    const uncmpSize = messages.reduce((total, msg) => total + 4 + msg.length, 0);
    const bufContent = Buffer.allocUnsafe(uncmpSize);
    const dwContent = new DataWriter(bufContent);
    messages.forEach(msg => dwContent.writeBytes(msg));
    const cmpContent = await gzipAsync(dwContent.data);

    const bufBatch = Buffer.allocUnsafe(11 + cmpContent.length);
    const dwBatch = new DataWriter(bufBatch);
    dwBatch.writeUInt8(0x80 | (CompressionTypes.Gzip << 4));
    dwBatch.writeUInt16(messages.length);
    dwBatch.writeUInt32(uncmpSize);
    dwBatch.writeBytes(cmpContent);
    return dwBatch.data;
}
