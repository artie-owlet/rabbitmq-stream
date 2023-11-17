/* eslint-disable @typescript-eslint/no-non-null-assertion */
function crc32GenerateTables(polynomial: number, numTables: number): Uint32Array {
    const table = new Uint32Array(256 * numTables);
    for (let i = 0; i < 256; i++) {
        let r = i;
        r = ((r & 1) * polynomial) ^ (r >>> 1);
        r = ((r & 1) * polynomial) ^ (r >>> 1);
        r = ((r & 1) * polynomial) ^ (r >>> 1);
        r = ((r & 1) * polynomial) ^ (r >>> 1);
        r = ((r & 1) * polynomial) ^ (r >>> 1);
        r = ((r & 1) * polynomial) ^ (r >>> 1);
        r = ((r & 1) * polynomial) ^ (r >>> 1);
        r = ((r & 1) * polynomial) ^ (r >>> 1);
        table[i] = r;
    }
    for (let i = 256; i < table.length; i++) {
        const value = table[i - 256]!;
        table[i] = table[value & 0xff]! ^ (value >>> 8);
    }
    return table;
}

const CRC32_TABLE = crc32GenerateTables(0xedb88320, 8);

export function crc32(data: ArrayBufferView): number {
    const byteLength = data.byteLength;
    const view = new DataView(data.buffer, data.byteOffset, byteLength);
    let r = ~0;
    let offset = 0;

    const toAlign = -view.byteOffset & 3;
    for (; offset < toAlign && offset < byteLength; offset++) {
        r = CRC32_TABLE[(r ^ view.getUint8(offset)) & 0xff]! ^ (r >>> 8);
    }
    /* c8 ignore start */
    if (offset === byteLength) {
        return r;
    }
    /* c8 ignore stop */

    offset = toAlign;

    let remainingBytes = byteLength - offset;
    for (; remainingBytes >= 8; offset += 8, remainingBytes -= 8) {
        r ^= view.getUint32(offset, true);
        const r2 = view.getUint32(offset + 4, true);
        r =
            CRC32_TABLE[0 * 256 + ((r2 >>> 24) & 0xff)]! ^
            CRC32_TABLE[1 * 256 + ((r2 >>> 16) & 0xff)]! ^
            CRC32_TABLE[2 * 256 + ((r2 >>> 8) & 0xff)]! ^
            CRC32_TABLE[3 * 256 + ((r2 >>> 0) & 0xff)]! ^
            CRC32_TABLE[4 * 256 + ((r >>> 24) & 0xff)]! ^
            CRC32_TABLE[5 * 256 + ((r >>> 16) & 0xff)]! ^
            CRC32_TABLE[6 * 256 + ((r >>> 8) & 0xff)]! ^
            CRC32_TABLE[7 * 256 + ((r >>> 0) & 0xff)]!;
    }

    for (let i = offset; i < byteLength; i++) {
        r = CRC32_TABLE[(r ^ view.getUint8(i)) & 0xff]! ^ (r >>> 8);
    }
    return (r ^ ~0) >>> 0;
}
