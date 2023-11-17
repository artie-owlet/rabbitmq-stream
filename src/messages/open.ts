import { Commands } from './constants';
import { ClientRequest } from './client-message';
import { ServerResponse } from './server-message';

export class OpenRequest extends ClientRequest {
    constructor(
        private vhost: string,
    ) {
        super(Commands.Open, 1);
    }

    protected override build(corrId: number): void {
        super.build(corrId);
        this.writeString(this.vhost);
    }
}

export class OpenResponse extends ServerResponse {
    public properties = new Map<string, string>();

    constructor(msg: Buffer) {
        super(msg);
        const size = this.readArraySize();
        for (let i = 0; i < size; ++i) {
            this.properties.set(this.readString(), this.readString());
        }
    }
}
