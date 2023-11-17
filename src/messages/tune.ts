import { Commands } from './constants';
import { ClientCommand } from './client-message';
import { ServerMessage } from './server-message';

export class ServerTune extends ServerMessage {
    public readonly frameMax;
    public readonly heartbeat;

    constructor(msg: Buffer) {
        super(msg);
        this.frameMax = this.readUInt32();
        this.heartbeat = this.readUInt32();
    }
}

export class ClientTune extends ClientCommand {
    constructor(
        private frameMax: number,
        private heartbeat: number,
    ) {
        super(Commands.Tune, 1);
    }

    protected override build(): void {
        super.build();
        this.writeUInt32(this.frameMax);
        this.writeUInt32(this.heartbeat);
    }
}
