import { readFileSync } from 'fs';
import { Socket } from 'net';
import { resolve as pathResolve } from 'path';

import { expect } from '@artie-owlet/chifir';

import { createFakeServer } from './fake-server';
import { promisifyEvent } from './promisify-event';
import { RabbitmqStreamManager } from './rabbitmq-stream-manager';
import { testName } from './utils';

import { Client, IClientOptions } from '../src';
import { Commands } from '../src/messages/constants';

const TEST_HOST = 'localhost';
const TEST_UNREACHABLE_HOST = '192.168.255.254';
const TEST_PORT = 4001;
const TEST_TLS_PORT = 4011;
const TEST_FAKE_PORT = 4000;
const TEST_COMMON_OPTS: IClientOptions = {
    username: 'guest',
    password: 'guest',
    vhost: '/',
};

describe('Client.createClient()', function _() {
    const streamManager = new RabbitmqStreamManager('rabbitmq-stream-client-ci-1');
    let cli: Client | undefined;
    let connectionName: string;
    let setNoDelayOrig: typeof Socket.prototype.setNoDelay;
    let noDelayValue: boolean | undefined;

    before(() => {
        // eslint-disable-next-line @typescript-eslint/unbound-method
        setNoDelayOrig = Socket.prototype.setNoDelay;
        Socket.prototype.setNoDelay = function setNoDelay(this: Socket, noDelay?: boolean): Socket {
            noDelayValue = noDelay;
            setNoDelayOrig.apply(this, [noDelay]);
            return this;
        };
    });

    after(() => {
        Socket.prototype.setNoDelay = setNoDelayOrig;
    });

    beforeEach(function _() {
        connectionName = `test-${testName(this)}`;
    });

    afterEach(() => {
        if (cli !== undefined) {
            cli.close();
        }
        cli = undefined;
        noDelayValue = undefined;
    });

    it('should connect', async () => {
        cli = await Client.createClient(TEST_HOST, TEST_PORT, {
            connectionName,
            connectTimeoutMs: 10,
            ...TEST_COMMON_OPTS,
        });
        await expect(streamManager.getStreamConnection(connectionName)).eventually.exist;
    });

    it('should connect over TLS', async () => {
        cli = await Client.createClient(TEST_HOST, TEST_TLS_PORT, {
            connectionName,
            ...TEST_COMMON_OPTS,
            tls: {
                pfx: readFileSync(pathResolve(process.cwd(), '.ci/client.pfx')),
            },
        });
        await expect(streamManager.getStreamConnection(connectionName)).eventually.exist
            .prop('ssl').eq(true);
    });

    it('should disable noDelay', async () => {
        cli = await Client.createClient(TEST_HOST, TEST_PORT, {
            ...TEST_COMMON_OPTS,
            noDelay: false,
        });
        expect(noDelayValue).eq(false);
    }).slow(200);

    it('should tune', async () => {
        cli = await Client.createClient(TEST_HOST, TEST_PORT, {
            connectionName,
            ...TEST_COMMON_OPTS,
            heartbeat: 10,
            frameMax: 8192,
        });
        await expect(streamManager.getStreamConnection(connectionName)).eventually.exist
            .prop('heartbeat').eq(10)
            .context()
            .prop('frame_max').eq(8192);
    });

    it('should throw on connect timeout', async () => {
        await expect(Client.createClient(TEST_UNREACHABLE_HOST, TEST_PORT, {
            ...TEST_COMMON_OPTS,
            connectTimeoutMs: 200,
        })).eventually.rejects(cli => cli.close())
            .instanceOf(Error)
            .prop('message').eq('Connect timeout');
    }).timeout(1000).slow(1000);

    it('should throw in case of invalid credentials', async () => {
        await expect(Client.createClient(TEST_HOST, TEST_PORT, {
            ...TEST_COMMON_OPTS,
            username: 'unknown',
        })).eventually.rejects(cli => cli.close())
            .instanceOf(Error);
    }).timeout(2000).slow(2000);

    it('should throw in case of forbidden vhost', async () => {
        await expect(Client.createClient(TEST_HOST, TEST_PORT, {
            ...TEST_COMMON_OPTS,
            vhost: 'unknown',
        })).eventually.rejects(cli => cli.close())
            .instanceOf(Error);
    }).timeout(2000).slow(2000);
});

describe('Client (bad server)', function _() {
    this.timeout(1000).slow(1000);

    let srv: Awaited<ReturnType<typeof createFakeServer>>;
    let cli: Client | undefined;

    beforeEach(async () => {
        srv = await createFakeServer(TEST_FAKE_PORT);
    });

    afterEach(async () => {
        await srv.close();
        if (cli) {
            cli.close();
        }
        cli = undefined;
    });

    it('.createClient() should throw on Tune timeout', async () => {
        srv.setNoTune();
        await expect(Client.createClient(TEST_HOST, TEST_FAKE_PORT, {
            ...TEST_COMMON_OPTS,
            requestTimeout: 0.2,
        })).eventually.rejects(cli => cli.close)
            .instanceOf(Error)
            .prop('message').eq('Tune timeout');
    });

    it('should abort pending requests', async () => {
        cli = await Client.createClient(TEST_HOST, TEST_FAKE_PORT, TEST_COMMON_OPTS);
        setImmediate(async () => await srv.close());
        await expect(cli.exchangeCommandVersion()).eventually.rejects()
            .instanceOf(Error)
            .prop('message').eq('Connection closed');
    });

    it('should abort long requests', async () => {
        cli = await Client.createClient(TEST_HOST, TEST_FAKE_PORT, {
            ...TEST_COMMON_OPTS,
            requestTimeout: 0.2,
        });
        await expect(cli.exchangeCommandVersion()).eventually.rejects()
            .instanceOf(Error)
            .prop('message').eq('Request timeout');
    });

    it('should handle unknown commands', async () => {
        cli = await Client.createClient(TEST_HOST, TEST_FAKE_PORT, TEST_COMMON_OPTS);
        setImmediate(() => srv.send(100, 1));
        await expect(promisifyEvent<Error>(cli, 'error')).eventually.instanceOf(Error);
    });

    it('should handle unknown command versions', async () => {
        cli = await Client.createClient(TEST_HOST, TEST_FAKE_PORT, TEST_COMMON_OPTS);
        setImmediate(() => srv.send(Commands.Deliver, 3));
        await expect(promisifyEvent<Error>(cli, 'error')).eventually.instanceOf(Error);
    });

    it('should close on heartbeat timeout', async () => {
        cli = await Client.createClient(TEST_HOST, TEST_FAKE_PORT, {
            ...TEST_COMMON_OPTS,
            heartbeat: 0.2,
        });
        await expect(promisifyEvent<Error>(cli, 'error')).eventually.instanceOf(Error)
            .prop('message').eq('Heartbeat timeout');
    });
});
