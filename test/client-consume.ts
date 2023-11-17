import { setTimeout } from 'timers/promises';
import { expect } from '@artie-owlet/chifir';

import { describeMethod } from './mocha-format';
import { promisifyEventArgs } from './promisify-event';
import { RabbitmqManager } from './rabbitmq-manager';
import { RabbitmqStreamManager } from './rabbitmq-stream-manager';
import { testName } from './utils';

import { Client, IClientOptions, Offset, OffsetTypes } from '../src';
import { StreamError } from '../src/stream-error';
import { createBatch } from '../src/messages/publish';
import { DeliverData } from '../src/messages/deliver';

type ConsumerUpdateArgs = [number, boolean, (offset: Offset) => void];

const TEST_HOST = 'localhost';
const TEST_PORT = 4001;
const TEST_COMMON_OPTS: IClientOptions = {
    username: 'guest',
    password: 'guest',
    vhost: `${Date.now()}-consume`,
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

    describeMethod(Client.prototype.subscribe, () => {
        it('should subscribe', async () => {
            await cli.subscribe(1, stream, { type: OffsetTypes.Last }, 3, new Map([
                ['app', 'test'],
            ]));
            await expect(streamManager.getConsumer(1, stream, TEST_COMMON_OPTS.vhost)).eventually.exist
                .prop('active').eq(true).context()
                .prop('credits').eq(3).context()
                .prop('properties').prop('app').eq('test');
        });

        it('should subscribe with absolute offset', async () => {
            await cli.subscribe(1, stream, { type: OffsetTypes.Absolute, value: 0n }, 3, new Map());
            await expect(streamManager.getConsumer(1, stream, TEST_COMMON_OPTS.vhost)).eventually.exist
                .prop('active').eq(true);
        });

        it('should subscribe with timestamp offset', async () => {
            await cli.subscribe(1, stream, { type: OffsetTypes.Timestamp, value: Date.now() }, 3, new Map());
            await expect(streamManager.getConsumer(1, stream, TEST_COMMON_OPTS.vhost)).eventually.exist
                .prop('active').eq(true);
        });
    });

    describeMethod(Client.prototype.credit, () => {
        beforeEach(async () => {
            await cli.subscribe(1, stream, { type: OffsetTypes.Last }, 3, new Map());
        });

        it('should increase credit', async () => {
            cli.credit(1, 2);
            await setTimeout(100);
            await expect(streamManager.getConsumer(1, stream, TEST_COMMON_OPTS.vhost)).eventually.exist
                .prop('credits').eq(5);
        });
    });

    describe('#on("creditError")', () => {
        it('should handle CreditResponse with error', async () => {
            setImmediate(() => cli.credit(1, 2));
            await expect(promisifyEventArgs<[number, number]>(cli, 'creditError')).eventually.eq([1, 0x04]);
        }).timeout(1000);
    });

    describe('offset', () => {
        it('should store and query offset', async () => {
            cli.storeOffset('test', stream, 123n);
            await setTimeout(100);
            await expect(cli.queryOffset('test', stream)).eventually.eq(123n);
        });

        it('#queryOffset() should throw if in case of unknown ref', async () => {
            await expect(cli.queryOffset('wrong', stream)).eventually.rejects()
                .instanceOf(StreamError)
                .prop('code').eq(0x13);
        });
    });

    describeMethod(Client.prototype.unsubscribe, () => {
        beforeEach(async () => {
            await cli.subscribe(1, stream, { type: OffsetTypes.Last }, 3, new Map());
        });

        it('should unsubscribe', async () => {
            await cli.unsubscribe(1);
            await expect(streamManager.getConsumer(1, stream, TEST_COMMON_OPTS.vhost)).eventually.eq(undefined);
        });
    });

    describe('#on("deliver")', () => {
        beforeEach(async () => {
            await cli.subscribe(1, stream, { type: OffsetTypes.Last }, 3, new Map());
        });

        afterEach(() => {
            cli.removeAllListeners('deliver');
        });

        it('should handle batches', async () => {
            await cli.declarePublisher(1, 'test', stream);
            const p = new Promise<Buffer[]>((res) => {
                const messages = [] as Buffer[];
                cli.on('deliver', async (_, data) => {
                    const msgs = await data.readMessages();
                    messages.push(...msgs);
                    if (messages.length === 3) {
                        res(messages);
                    }
                });
            });
            setImmediate(async () => cli.publish(1, [
                { id: 1n, payload: await createBatch([Buffer.from('test1'), Buffer.from('test22')]), batch: true },
                { id: 2n, payload: Buffer.from('test333') },
            ]));
            await expect(p).eventually.eq([
                Buffer.from('test1'),
                Buffer.from('test22'),
                Buffer.from('test333'),
            ]);
        });
    });

    describe('#on("consumerUpdate")', () => {
        let pDeliver: Promise<[unknown, DeliverData]>;
        let pUpdate: Promise<ConsumerUpdateArgs>;
        let update: ConsumerUpdateArgs;

        beforeEach(async () => {
            await manager.publish('test000', stream, TEST_COMMON_OPTS.vhost);
            await manager.publish('test111', stream, TEST_COMMON_OPTS.vhost);

            pDeliver = promisifyEventArgs<[unknown, DeliverData]>(cli, 'deliver');
            pUpdate = promisifyEventArgs<ConsumerUpdateArgs>(cli, 'consumerUpdate');
            await cli.subscribe(1, stream, { type: OffsetTypes.First }, 3, new Map([
                ['single-active-consumer', 'true'],
                ['name', 'sub-1'],
            ]));
            update = await pUpdate;
        });

        it('should handle ConsumerUpdate', async () => {
            const [subId, active, response] = update;
            expect(subId).eq(1);
            expect(active).eq(true);
            response({ type: OffsetTypes.Absolute, value: 2n });

            await manager.publish('test222', stream, TEST_COMMON_OPTS.vhost);
            const [, data] = await pDeliver;
            const messages = await data.readMessages();
            expect(messages.length).eq(1);
            expect(messages[0].toString()).match(/test222/);
        }).timeout(5000);

        it('should handle ConsumerUpdate with timestamp offset', async () => {
            const [, active, response] = update;
            expect(active).eq(true);
            response({ type: OffsetTypes.Timestamp, value: Date.now() });

            await manager.publish('test222', stream, TEST_COMMON_OPTS.vhost);
            const [, data] = await pDeliver;
            const messages = await data.readMessages();
            expect(messages.length).eq(1);
            expect(messages[0].toString()).match(/test222/);
        }).timeout(5000);

        // it('should handle consume rejection', async () => {
        //     update.reject();
        //     await setTimeout(100);
        //     await expect(streamManager.getConsumer(1, stream, TEST_COMMON_OPTS.vhost)).eventually.eq(undefined);
        // });
    });
});
