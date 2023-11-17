import { Commands } from './constants';
import { ClientRequest } from './client-message';
import { ServerResponse } from './server-message';

export class StreamStatsRequest extends ClientRequest {
    constructor(
        private stream: string,
    ) {
        super(Commands.StreamStats, 1);
    }

    protected override build(corrId: number): void {
        super.build(corrId);
        this.writeString(this.stream);
    }
}

export class StreamStatsResponse extends ServerResponse {
    public readonly stats = new Map<string, bigint>();

    constructor(msg: Buffer) {
        super(msg);
        const size = this.readArraySize();
        for (let i = 0; i < size; ++i) {
            this.stats.set(this.readString(), this.readInt64());
        }
    }
}
