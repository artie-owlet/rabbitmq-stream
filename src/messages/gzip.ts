import { gunzip } from 'zlib';

export function decodeGzip(input: Buffer): Promise<Buffer> {
    return new Promise((res, rej) => {
        gunzip(input, (err, output) => {
            /* c8 ignore start */
            if (err) {
                rej(err);
            /* c8 ignore stop */
            } else {
                res(output);
            }
        });
    });
}
