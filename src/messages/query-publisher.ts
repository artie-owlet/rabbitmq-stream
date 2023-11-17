import { Commands } from './constants';
import { ClientRequest } from './client-message';
import { ServerResponse } from './server-message';

export class QueryPublisherRequest extends ClientRequest {
    constructor(
        private reference: string,
        private stream: string,
    ) {
        super(Commands.QueryPublisherSequence, 1);
    }

    protected override build(corrId: number): void {
        super.build(corrId);
        this.writeString(this.reference);
        this.writeString(this.stream);
    }
}

export class QueryPublisherResponse extends ServerResponse {
    public readonly seq: bigint;

    constructor(msg: Buffer) {
        super(msg);
        this.seq = this.readUInt64();
    }
}
