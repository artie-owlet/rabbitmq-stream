import { Commands } from './constants';
import { ClientRequest } from './client-message';
import { ServerResponse } from './server-message';

export interface ICommandVersion {
    key: number;
    minVersion: number;
    maxVersion: number;
}

const cmds: ICommandVersion[] = [
    { key: Commands.DeclarePublisher, minVersion: 1, maxVersion: 1 },
    { key: Commands.Publish, minVersion: 1, maxVersion: 1 },
    { key: Commands.PublishConfirm, minVersion: 1, maxVersion: 1 },
    { key: Commands.PublishError, minVersion: 1, maxVersion: 1 },
    { key: Commands.QueryPublisherSequence, minVersion: 1, maxVersion: 1 },
    { key: Commands.DeletePublisher, minVersion: 1, maxVersion: 1 },
    { key: Commands.Subscribe, minVersion: 1, maxVersion: 1 },
    { key: Commands.Deliver, minVersion: 1, maxVersion: 2 },
    { key: Commands.Credit, minVersion: 1, maxVersion: 1 },
    { key: Commands.StoreOffset, minVersion: 1, maxVersion: 1 },
    { key: Commands.QueryOffset, minVersion: 1, maxVersion: 1 },
    { key: Commands.Unsubscribe, minVersion: 1, maxVersion: 1 },
    { key: Commands.CreateStream, minVersion: 1, maxVersion: 1 },
    { key: Commands.DeleteStream, minVersion: 1, maxVersion: 1 },
    { key: Commands.Metadata, minVersion: 1, maxVersion: 1 },
    { key: Commands.MetadataUpdate, minVersion: 1, maxVersion: 1 },
    { key: Commands.PeerProperties, minVersion: 1, maxVersion: 1 },
    { key: Commands.SaslHandshake, minVersion: 1, maxVersion: 1 },
    { key: Commands.SaslAuthenticate, minVersion: 1, maxVersion: 1 },
    { key: Commands.Tune, minVersion: 1, maxVersion: 1 },
    { key: Commands.Open, minVersion: 1, maxVersion: 1 },
    { key: Commands.Close, minVersion: 1, maxVersion: 1 },
    { key: Commands.Heartbeat, minVersion: 1, maxVersion: 1 },
    { key: Commands.Route, minVersion: 1, maxVersion: 1 },
    { key: Commands.Partitions, minVersion: 1, maxVersion: 1 },
    { key: Commands.ConsumerUpdate, minVersion: 1, maxVersion: 1 },
    { key: Commands.ExchangeCommandVersions, minVersion: 1, maxVersion: 1 },
    { key: Commands.StreamStats, minVersion: 1, maxVersion: 1 },
];

export class CommandVersionsExchangeRequest extends ClientRequest {
    constructor() {
        super(Commands.ExchangeCommandVersions, 1);
    }

    protected override build(corrId: number): void {
        super.build(corrId);
        this.writeArraySize(cmds.length);
        cmds.forEach((cmd) => {
            this.writeUInt16(cmd.key);
            this.writeUInt16(cmd.minVersion);
            this.writeUInt16(cmd.maxVersion);
        });
    }
}

export class CommandVersionsExchangeResponse extends ServerResponse {
    public readonly commands = [] as ICommandVersion[];

    constructor(msg: Buffer) {
        super(msg);
        const size = this.readArraySize();
        for (let i = 0; i < size; ++i) {
            this.commands.push({
                key: this.readUInt16(),
                minVersion: this.readUInt16(),
                maxVersion: this.readUInt16(),
            });
        }
    }
}
