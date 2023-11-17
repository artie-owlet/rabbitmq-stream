import { Commands } from './constants';
import { ClientRequest } from './client-message';
import { ServerResponse } from './server-message';

export class PartitionsRequest extends ClientRequest {
    constructor(
        private superStream: string,
    ) {
        super(Commands.Partitions, 1);
    }

    protected override build(corrId: number): void {
        super.build(corrId);
        this.writeString(this.superStream);
    }
}

export class PartitionsResponse extends ServerResponse {
    public readonly streams = [] as string[];

    constructor(msg: Buffer) {
        super(msg);
        const size = this.readArraySize();
        for (let i = 0; i < size; ++i) {
            this.streams.push(this.readString());
        }
    }
}
