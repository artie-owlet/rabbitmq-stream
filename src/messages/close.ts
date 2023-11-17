import { Commands, RESPONSE_CODE_OK } from './constants';
import { ServerRequest } from './server-message';
import { ClientResponse } from './client-message';

export class CloseRequest extends ServerRequest {
    public readonly reason: string;

    constructor(msg: Buffer) {
        super(msg);
        this.reason = this.readString();
    }
}

export class CloseResponse extends ClientResponse {
    constructor(
        corrId: number,
    ) {
        super(Commands.Close, 1, corrId, RESPONSE_CODE_OK);
    }
}
