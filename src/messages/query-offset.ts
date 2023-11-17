import { Commands } from './constants';
import { ClientRequest } from './client-message';
import { ServerResponse } from './server-message';

export class QueryOffsetRequest extends ClientRequest {
    constructor(
        private reference: string,
        private stream: string,
    ) {
        super(Commands.QueryOffset, 1);
    }

    protected override build(corrId: number): void {
        super.build(corrId);
        this.writeString(this.reference);
        this.writeString(this.stream);
    }
}

export class QueryOffsetResponse extends ServerResponse {
    public readonly offsetValue: bigint;

    constructor(msg: Buffer) {
        super(msg);
        this.offsetValue = this.readUInt64();
    }
}
