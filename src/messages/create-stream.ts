import { Commands } from './constants';
import { ClientRequest } from './client-message';

export class CreateStreamRequest extends ClientRequest {
    constructor(
        private stream: string,
        private args: Map<string, string>,
    ) {
        super(Commands.CreateStream, 1);
    }

    protected override build(corrId: number): void {
        super.build(corrId);
        this.writeString(this.stream);
        this.writeArraySize(this.args.size);
        this.args.forEach((value, key) => {
            this.writeString(key);
            this.writeString(value);
        });
    }
}
