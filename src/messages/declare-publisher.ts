import { Commands } from './constants';
import { ClientRequest } from './client-message';

export class DeclarePublisherRequest extends ClientRequest {
    constructor(
        private publisherId: number,
        private reference: string,
        private stream: string,
    ) {
        super(Commands.DeclarePublisher, 1);
    }

    protected override build(corrId: number): void {
        super.build(corrId);
        this.writeUInt8(this.publisherId);
        this.writeString(this.reference);
        this.writeString(this.stream);
    }
}
