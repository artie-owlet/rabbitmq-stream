import { Commands } from './constants';
import { ClientRequest } from './client-message';

export class DeleteStreamRequest extends ClientRequest {
    constructor(
        private stream: string,
    ) {
        super(Commands.DeleteStream, 1);
    }

    protected override build(corrId: number): void {
        super.build(corrId);
        this.writeString(this.stream);
    }
}
