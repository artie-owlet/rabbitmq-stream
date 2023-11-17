import { expect } from '@artie-owlet/chifir';

import { describeMethod } from './mocha-format';
import { promisifyEventArgs } from './promisify-event';
import { RabbitmqManager } from './rabbitmq-manager';
import { RabbitmqStreamManager } from './rabbitmq-stream-manager';
import { testName } from './utils';

import { Client, IClientOptions, IPublishingError } from '../src';
import { createBatch } from '../src/messages/publish';

const TEST_HOST = 'localhost';
const TEST_PORT = 4001;
const TEST_COMMON_OPTS: IClientOptions = {
    username: 'guest',
    password: 'guest',
    vhost: `${Date.now()}-publish`,
};

describe('Client', function _() {
    const manager = new RabbitmqManager();
    const streamManager = new RabbitmqStreamManager('rabbitmq-stream-client-ci-1');
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
        await manager.createStream(stream, TEST_COMMON_OPTS.vhost);
    });

    afterEach(() => {
        cli.close();
    });

    describeMethod(Client.prototype.declarePublisher, () => {
        it('should declare publisher', async () => {
            await cli.declarePublisher(1, 'test-pub', stream);
            await expect(streamManager.getPublisher('test-pub', stream, TEST_COMMON_OPTS.vhost)).eventually.exist
                .prop('publisher_id').eq(1);
        });
    });

    describeMethod(Client.prototype.deletePublisher, () => {
        beforeEach(async () => {
            await cli.declarePublisher(1, 'test-pub', stream);
        });

        it('should delete publisher', async () => {
            await cli.deletePublisher(1);
            await expect(streamManager.getPublisher('test-pub', stream, TEST_COMMON_OPTS.vhost)).eventually
                .eq(undefined);
        });
    });

    describeMethod(Client.prototype.publish, () => {
        beforeEach(async () => {
            await cli.declarePublisher(1, 'test-pub', stream);
        });

        it('should publish single message', async () => {
            setImmediate(() => cli.publish(1, [{
                id: 1n,
                payload: Buffer.from('test'),
            }]));
            await expect(promisifyEventArgs<[number, bigint[]]>(cli, 'publishConfirm')).eventually.eq([1, [1n]]);
        });

        it('should publish messages in batch', async () => {
            setImmediate(async () => cli.publish(1, [{
                id: 1n,
                payload: await createBatch([Buffer.from('test1'), Buffer.from('test2')]),
                batch: true,
            }]));
            await expect(promisifyEventArgs<[number, bigint[]]>(cli, 'publishConfirm')).eventually.eq([1, [1n]]);
        });

        it('should publish mixed', async () => {
            setImmediate(async () => cli.publish(1, [{
                id: 1n,
                payload: Buffer.from('test'),
            }, {
                id: 2n,
                payload: await createBatch([Buffer.from('test1'), Buffer.from('test2')]),
                batch: true,
            }]));
            await expect(promisifyEventArgs<[number, bigint[]]>(cli, 'publishConfirm')).eventually.eq([1, [1n, 2n]]);
        });
    });

    describeMethod(Client.prototype.queryPublisherSequence, () => {
        beforeEach(async () => {
            await cli.declarePublisher(1, 'test-pub', stream);
        });

        it('should query publisher', async () => {
            cli.publish(1, [{
                id: 10n,
                payload: Buffer.from('test'),
            }]);
            await expect(cli.queryPublisherSequence('test-pub', stream)).eventually.eq(10n);
        });
    });

    describe('#on("publishError")', () => {
        it('should handle PublishError', async () => {
            setImmediate(() => cli.publish(1, [{
                id: 1n,
                payload: Buffer.from('test'),
            }]));
            await expect(promisifyEventArgs<[number, IPublishingError[]]>(cli, 'publishError')).eventually.eq([1, [{
                id: 1n,
                code: 0x12,
            }]]);
        }).timeout(1000);
    });
});
