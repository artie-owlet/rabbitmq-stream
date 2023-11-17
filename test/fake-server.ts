import { createServer, Server, Socket } from 'net';

import { ClientCommand } from '../src/messages/client-message';

const fakeMessages = {
    // eslint-disable-next-line max-len
    ppeResp: Buffer.from('000000f58011000100000001000100000006000c636c75737465725f6e616d65000b7261626269742d686f6c650009636f707972696768740037436f707972696768742028632920323030372d3230323320564d776172652c20496e632e206f722069747320616666696c69617465732e000b696e666f726d6174696f6e00394c6963656e73656420756e64657220746865204d504c20322e302e20576562736974653a2068747470733a2f2f7261626269746d712e636f6d0008706c6174666f726d000f45726c616e672f4f54502032352e33000770726f6475637400085261626269744d51000776657273696f6e0007332e31312e3130', 'hex'),
    saslHandshakeResp: Buffer.from('0000001f80120001000000020001000000020008414d51504c41494e0005504c41494e', 'hex'),
    saslAuthResp: Buffer.from('0000000a80130001000000030001', 'hex'),
    tune: Buffer.from('0000000c00140001001000000000003c', 'hex'),
    // eslint-disable-next-line max-len
    openResp: Buffer.from('0000003d8015000100000004000100000002000f616476657274697365645f706f7274000435353532000f616476657274697365645f686f73740005686f737431', 'hex'),
};

interface IFakeServerOpts {
    silent?: boolean;
    noTune?: boolean;
}

class FakeServer {
    private opts = {} as IFakeServerOpts;
    private sock?: Socket;
    private totalMsgCount = 0;

    constructor(
        private srv: Server,
    ) {
        srv.on('connection', (sock) => {
            this.sock = sock;
            sock.on('error', err => console.log(err));
            sock.on('data', this.onData.bind(this));
            sock.on('end', () => {
                if (sock.allowHalfOpen) {
                    sock.end();
                }
            });
        });
    }

    public setSilent(): void {
        this.opts.silent = true;
    }

    public setNoTune(): void {
        this.opts.noTune = true;
    }

    public send(key: number, version: number): void {
        if (this.sock) {
            this.sock.write(new ClientCommand(key, version).serialize());
        }
    }

    public async close(): Promise<void> {
        if (!this.srv.listening) {
            return;
        }

        return new Promise((res) => {
            this.srv.once('close', () => res());
            this.srv.close();
            if (this.sock && !this.sock.closed) {
                this.sock.end();
            }
        });
    }

    private onData(msg: Buffer): void {
        if (!this.sock) {
            return;
        }

        if (this.opts.silent) {
            return;
        }

        while (msg.length > 0) {
            const size = msg.readUInt32BE();
            msg = msg.subarray(size + 4);
            ++this.totalMsgCount;
        }
        switch (this.totalMsgCount) {
            case 1:
                this.sock.write(fakeMessages.ppeResp);
                break;
            case 2:
                this.sock.write(fakeMessages.saslHandshakeResp);
                break;
            case 3:
                this.sock.write(fakeMessages.saslAuthResp);
                if (!this.opts.noTune) {
                    this.sock.write(fakeMessages.tune);
                }
                break;
            case 5:
                this.sock.write(fakeMessages.openResp);
                break;
        }
    }
}

export async function createFakeServer(port: number): Promise<FakeServer> {
    return new Promise<FakeServer>((res, rej) => {
        const srv = createServer();
        const fake = new FakeServer(srv);
        srv.on('listening', () => res(fake));
        srv.on('error', (err) => rej(err));
        srv.listen(port, 'localhost');
    });
}
