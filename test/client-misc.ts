import { expect } from '@artie-owlet/chifir';

import { describeMethod } from './mocha-format';
import { RabbitmqManager } from './rabbitmq-manager';

import { Client, IClientOptions } from '../src';
import { Commands } from '../src/messages/constants';

const TEST_HOST = 'localhost';
const TEST_PORT = 4001;
const TEST_COMMON_OPTS: IClientOptions = {
    username: 'guest',
    password: 'guest',
    vhost: `${Date.now()}-misc`,
};

describe('Client', function _() {
    const manager = new RabbitmqManager();
    let cli: Client;

    before(async () => {
        await manager.addVhost(TEST_COMMON_OPTS.vhost);
    });

    after(async () => {
        await manager.deleteVhost(TEST_COMMON_OPTS.vhost);
    });

    beforeEach(async function _() {
        cli = await Client.createClient(TEST_HOST, TEST_PORT, TEST_COMMON_OPTS);
        cli.on('error', (err) => console.error(err.message));
    });

    afterEach(() => {
        cli.close();
    });

    describeMethod(Client.prototype.streamStats, () => {
        beforeEach(async () => {
            await manager.createStream('test-stats', TEST_COMMON_OPTS.vhost);
            await manager.publish('test1', 'test-stats', TEST_COMMON_OPTS.vhost);
            await manager.publish('test2', 'test-stats', TEST_COMMON_OPTS.vhost);
        });

        it('should return stream stats', async () => {
            await expect(cli.streamStats('test-stats')).eventually.eq(new Map([
                ['committed_chunk_id', 1n],
                ['first_chunk_id', 0n],
                ['last_chunk_id', 1n],
            ]));
        });
    });

    describeMethod(Client.prototype.exchangeCommandVersion, () => {
        it('should return server command versions', async () => {
            const versions = await cli.exchangeCommandVersion();
            versions.forEach((v) => {
                expect(Commands[v.key]).exist;
                expect(v).prop('minVersion').eq(1);
            });
        });
    });
});
