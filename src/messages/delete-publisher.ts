import { Commands } from './constants';
import { ClientRequest } from './client-message';

export class DeletePublisherRequest extends ClientRequest {
    constructor(
        private publisherId: number,
    ) {
        super(Commands.DeletePublisher, 1);
    }

    protected override build(corrId: number): void {
        super.build(corrId);
        this.writeUInt8(this.publisherId);
    }
}
