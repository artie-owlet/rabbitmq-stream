export class DataReader {
    constructor(
        private _data: Buffer,
        private _offset: number,
    ) {
    }

    protected getOffset(): number {
        return this._offset;
    }

    public readInt8(): number {
        const n = this._data.readInt8(this._offset);
        this._offset += 1;
        return n;
    }

    public readInt16(): number {
        const n = this._data.readInt16BE(this._offset);
        this._offset += 2;
        return n;
    }

    public readInt32(): number {
        const n = this._data.readInt32BE(this._offset);
        this._offset += 4;
        return n;
    }

    public readInt64(): bigint {
        const n = this._data.readBigInt64BE(this._offset);
        this._offset += 8;
        return n;
    }

    public readUInt8(): number {
        const n = this._data.readUInt8(this._offset);
        this._offset += 1;
        return n;
    }

    public readUInt16(): number {
        const n = this._data.readUInt16BE(this._offset);
        this._offset += 2;
        return n;
    }

    public readUInt32(): number {
        const n = this._data.readUInt32BE(this._offset);
        this._offset += 4;
        return n;
    }

    public readUInt64(): bigint {
        const n = this._data.readBigUInt64BE(this._offset);
        this._offset += 8;
        return n;
    }

    public readBytes(): Buffer {
        const size = this.readInt32();
        if (size < 0) {
            return Buffer.from('');
        }
        /* c8 ignore start */
        if (this._offset + size > this._data.length) {
            throw new Error('Failed to read bytes');
        }
        /* c8 ignore stop */
        const bytes = this._data.subarray(this._offset, this._offset + size);
        this._offset += size;
        return bytes;
    }

    public readString(): string {
        const size = this.readInt16();
        if (size < 0) {
            return '';
        }
        /* c8 ignore start */
        if (this._offset + size > this._data.length) {
            throw new Error('Failed to read string');
        }
        /* c8 ignore stop */
        const str = this._data.toString('utf8', this._offset, this._offset + size);
        this._offset += size;
        return str;
    }

    public readArraySize(): number {
        return this.readInt32();
    }

    public shift(n: number): void {
        this._offset += n;
    }

    public unshift(n: number): void {
        this._offset -= n;
    }
}
