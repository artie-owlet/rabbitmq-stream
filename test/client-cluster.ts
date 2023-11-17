import { expect } from '@artie-owlet/chifir';

import { describeMethod } from './mocha-format';
import { promisifyEventArgs } from './promisify-event';
import { RabbitmqManager } from './rabbitmq-manager';
import { RabbitmqStreamManager } from './rabbitmq-stream-manager';
import { testName } from './utils';

import { Client, IClientOptions } from '../src';

const TEST_HOST = 'localhost';
const TEST_PORT = 4001;
const TEST_COMMON_OPTS: IClientOptions = {
    username: 'guest',
    password: 'guest',
    vhost: `${Date.now()}-cluster`,
};

describe('Client', function _() {
    const manager = new RabbitmqManager();
    const streamManager = new RabbitmqStreamManager('rabbitmq-stream-client-ci-1');
    let cli: Client;
    let stream: string;

    before(async () => {
        await manager.addVhost(TEST_COMMON_OPTS.vhost);
        await streamManager.createSuperStream('test-super', TEST_COMMON_OPTS.vhost);
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

    describe('#advertisedHost', () => {
        it('should return advertised host', () => {
            expect(cli.advertisedHost).eq('host1');
        });
    });

    describeMethod(Client.prototype.metadata, () => {
        beforeEach(async () => {
            await manager.createStream(stream, TEST_COMMON_OPTS.vhost);
            await Promise.all([
                streamManager.addReplica(stream, 'rabbit@host2', TEST_COMMON_OPTS.vhost),
                streamManager.addReplica(stream, 'rabbit@host3', TEST_COMMON_OPTS.vhost),
            ]);
        });

        it('should return stream metadata', async () => {
            const res = await cli.metadata([stream]);
            expect(res.get(stream)).exist
                .prop('leader').eq('host1').context()
                .prop('replicas').eq(['host2', 'host3']);
        });

        it('should handle non-existent streams', async () => {
            const res = await cli.metadata([stream, 'not-exists']);
            expect(res.get('not-exists')).eq(undefined);
        });
    });

    describe('#on("metadataUpdate")', () => {
        beforeEach(async () => {
            await manager.createStream(stream, TEST_COMMON_OPTS.vhost);
            await cli.declarePublisher(1, 'test', stream);
        });

        it('should handle MetadataUpdate', async () => {
            setImmediate(async () => await streamManager.deleteReplica(stream, 'rabbit@host1', TEST_COMMON_OPTS.vhost));
            await expect(promisifyEventArgs<[string, number]>(cli, 'metadataUpdate')).eventually.eq([stream, 0x06]);
        });
    });

    describeMethod(Client.prototype.partitions, () => {
        it('should return partitions', async () => {
            await expect(cli.partitions('test-super')).eventually.eq(['test-super-0', 'test-super-1', 'test-super-2']);
        });
    });

    describeMethod(Client.prototype.route, () => {
        it('should return stream for routing key', async () => {
            await expect(cli.route('0', 'test-super')).eventually.eq(['test-super-0']);
        });
    });
});
