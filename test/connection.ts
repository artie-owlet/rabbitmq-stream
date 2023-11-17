import { createServer } from 'net';
import { setImmediate as pause } from 'timers/promises';
import { expect } from '@artie-owlet/chifir';

import { promisifyEvent } from './promisify-event';

import { Connection } from '../src/connection';

describe('Connection', function _() {
    this.timeout(1000).slow(1000);

    let srv: ReturnType<typeof createServer>;
    const outMessages = [] as Buffer[];

    before(async () => {
        for (let i = 0; i < 5; ++i) {
            const buf = Buffer.allocUnsafe(9);
            buf.writeUInt32BE(5);
            buf.write(`test${i}`, 4);
            outMessages.push(buf);
        }

        srv = createServer(async (sock) => {
            sock.write(outMessages[0]);
            await pause();
            sock.write(Buffer.concat([outMessages[1], outMessages[2].subarray(0, 2)]));
            await pause();
            sock.write(Buffer.concat([outMessages[2].subarray(2), outMessages[3].subarray(0, 5)]));
            await pause();
            sock.write(Buffer.concat([outMessages[3].subarray(5), outMessages[4]]));
            sock.end();
        });
        setImmediate(() => srv.listen(4100, 'localhost'));
        await promisifyEvent(srv, 'listening');
    });

    after(() => {
        srv.close();
    });

    it('should handle incoming messages', async () => {
        const conn = new Connection('localhost', 4100, {});
        await expect(new Promise((res) => {
            const messages = [] as Buffer[];
            conn.on('message', (msg) => {
                messages.push(msg);
                if (messages.length === 5) {
                    res(messages);
                }
            });
        })).eventually.eq(outMessages);
    });
});
