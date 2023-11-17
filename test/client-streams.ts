import { expect } from '@artie-owlet/chifir';

import { RabbitmqManager } from './rabbitmq-manager';
import { testName } from './utils';

import { Client, IClientOptions } from '../src';

const TEST_HOST = 'localhost';
const TEST_PORT = 4001;
const TEST_COMMON_OPTS: IClientOptions = {
    username: 'guest',
    password: 'guest',
    vhost: `${Date.now()}-streams`,
};

describe('Client', function _() {
    const manager = new RabbitmqManager();
    let cli: Client;
    let stream: string;

    before(async () => {
        await manager.addVhost(TEST_COMMON_OPTS.vhost);
    });

    after(async () => {
        await manager.deleteVhost(TEST_COMMON_OPTS.vhost);
    });

    beforeEach(async function _() {
        stream = testName(this);
        cli = await Client.createClient(TEST_HOST, TEST_PORT, TEST_COMMON_OPTS);
        cli.on('error', (err) => console.error(err.message));
    });

    afterEach(() => {
        cli.close();
    });

    describe('#create()', () => {
        it('should create stream', async () => {
            await cli.createStream(stream, new Map());
            const info = await manager.getStream(stream, TEST_COMMON_OPTS.vhost);
            expect(info).exist
                .prop('arguments')
                .prop('x-queue-type').eq('stream');
        });

        it('should create stream with args', async () => {
            const args = new Map([
                ['initial-cluster-size', '3'],
                ['max-age', '1D'],
                ['max-length-bytes', '100000'],
                ['queue-leader-locator', 'balanced'],
                ['stream-max-segment-size-bytes', '10000'],
            ]);
            await cli.createStream(stream, args);
            const info = await expect(manager.getStream(stream, TEST_COMMON_OPTS.vhost)).eventually.exist.value;
            args.forEach((value, key) => expect(info.arguments[`x-${key}`].toString()).eq(value));
        });
    });

    describe('#delete()', () => {
        beforeEach(async () => {
            await cli.createStream(stream, new Map());
        });

        it('should delete stream', async () => {
            await cli.deleteStream(stream);
            await expect(manager.getStream(stream, TEST_COMMON_OPTS.vhost)).eventually.eq(undefined);
        });
    });
});
