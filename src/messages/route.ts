import { Commands } from './constants';
import { ClientRequest } from './client-message';
import { ServerResponse } from './server-message';

export class RouteRequest extends ClientRequest {
    constructor(
        private routingKey: string,
        private superStream: string,
    ) {
        super(Commands.Route, 1);
    }

    protected override build(corrId: number): void {
        super.build(corrId);
        this.writeString(this.routingKey);
        this.writeString(this.superStream);
    }
}

export class RouteResponse extends ServerResponse {
    public readonly streams = [] as string[];

    constructor(msg: Buffer) {
        super(msg);
        const size = this.readArraySize();
        for (let i = 0; i < size; ++i) {
            this.streams.push(this.readString());
        }
    }
}
