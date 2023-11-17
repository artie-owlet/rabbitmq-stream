import { Commands } from './constants';
import { ClientRequest } from './client-message';

export class UnsubscribeRequest extends ClientRequest {
    constructor(
        private subscriptionId: number,
    ) {
        super(Commands.Unsubscribe, 1);
    }

    protected override build(corrId: number): void {
        super.build(corrId);
        this.writeUInt8(this.subscriptionId);
    }
}
