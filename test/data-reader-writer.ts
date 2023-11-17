import { expect } from '@artie-owlet/chifir';

import { ClientCommand } from '../src/messages/client-message';
import { ServerMessage } from '../src/messages/server-message';

const TEST_INT8 = 0x40 + 8;
const TEST_INT16 = 0x4000 + 16;
const TEST_INT32 = 0x40000000 + 32;
const TEST_INT64 = 123n;
const TEST_UINT8 = 0x80 + 8;
const TEST_UINT16 = 0x8000 + 16;
const TEST_UINT32 = 0x80000000 + 32;
const TEST_BUFFER = Buffer.from('test');

class TestWriter extends ClientCommand {
    constructor() {
        super(0, 0);
    }

    protected override build(): void {
        super.build();
        this.writeInt8(TEST_INT8);
        this.writeInt16(TEST_INT16);
        this.writeInt32(TEST_INT32);
        this.writeInt64(TEST_INT64);
        this.writeUInt8(TEST_UINT8);
        this.writeUInt16(TEST_UINT16);
        this.writeUInt32(TEST_UINT32);
        this.writeUInt64(TEST_INT64);
        this.writeBytes(TEST_BUFFER);
        this.writeString('test');
        this.writeString(null);
        this.writeBytes(null);
    }
}

class TestReader extends ServerMessage {
    constructor(msg: Buffer) {
        super(msg);
        expect(this.readInt8()).eq(TEST_INT8);
        expect(this.readInt16()).eq(TEST_INT16);
        expect(this.readInt32()).eq(TEST_INT32);
        expect(this.readInt64()).eq(TEST_INT64);
        expect(this.readUInt8()).eq(TEST_UINT8);
        expect(this.readUInt16()).eq(TEST_UINT16);
        expect(this.readUInt32()).eq(TEST_UINT32);
        expect(this.readUInt64()).eq(TEST_INT64);
        expect(this.readBytes()).eq(TEST_BUFFER);
        expect(this.readString()).eq('test');
        expect(this.readBytes()).eq(Buffer.from(''));
        expect(this.readString()).eq('');
    }
}

describe('ClientMessage, ServerMessage', () => {
    it('should parse and serialize data', () => {
        const cmd = new TestWriter();
        const msg = cmd.serialize();
        new TestReader(msg);
    });
});
