export class DataWriter {
    private offset = 0;

    constructor(
        public data: Buffer,
    ) {
    }

    public writeInt8(n: number): void {
        this.offset = this.data.writeInt8(n, this.offset);
    }

    public writeInt16(n: number): void {
        this.offset = this.data.writeInt16BE(n, this.offset);
    }

    public writeInt32(n: number): void {
        this.offset = this.data.writeInt32BE(n, this.offset);
    }

    public writeInt64(n: bigint): void {
        this.offset = this.data.writeBigInt64BE(n, this.offset);
    }

    public writeUInt8(n: number): void {
        this.offset = this.data.writeUInt8(n, this.offset);
    }

    public writeUInt16(n: number): void {
        this.offset = this.data.writeUInt16BE(n, this.offset);
    }

    public writeUInt32(n: number): void {
        this.offset = this.data.writeUInt32BE(n, this.offset);
    }

    public writeUInt64(n: bigint): void {
        this.offset = this.data.writeBigUInt64BE(n, this.offset);
    }

    public writeBytes(bytes: Buffer | null): void {
        if (bytes !== null) {
            this.writeInt32(bytes.length);
            this.offset += bytes.copy(this.data, this.offset);
        } else {
            this.writeInt32(-1);
        }
    }

    public writeString(str: string | null): void {
        if (str !== null) {
            const size = this.data.write(str, this.offset + 2);
            this.writeInt16(size);
            this.offset += size;
        } else {
            this.writeInt16(-1);
        }
    }

    public writeRawData(bytes: Buffer): void {
        this.offset += bytes.copy(this.data, this.offset);
    }
}
