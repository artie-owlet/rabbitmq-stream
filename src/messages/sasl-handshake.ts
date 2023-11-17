import { Commands } from './constants';
import { ClientRequest } from './client-message';
import { ServerResponse } from './server-message';

export class SaslHandshakeRequest extends ClientRequest {
    constructor() {
        super(Commands.SaslHandshake, 1);
    }
}

export class SaslHandshakeResponse extends ServerResponse {
    public readonly mechanisms: string[] = [];

    constructor(msg: Buffer) {
        super(msg);
        const size = this.readArraySize();
        for (let i = 0; i < size; ++i) {
            this.mechanisms.push(this.readString());
        }
    }
}
