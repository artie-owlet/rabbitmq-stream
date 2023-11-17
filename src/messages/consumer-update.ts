import { Commands, RESPONSE_CODE_OK } from './constants';
import { ServerRequest } from './server-message';
import { ClientResponse } from './client-message';
import { Offset, OffsetTypes } from './offset';

export class ConsumerUpdateRequest extends ServerRequest {
    public readonly subscriptionId: number;
    public readonly active: boolean;

    constructor(msg: Buffer) {
        super(msg);
        this.subscriptionId = this.readUInt8();
        this.active = this.readUInt8() !== 0 ? true /* c8 ignore next */ : false;
    }
}

export class ConsumerUpdateResponse extends ClientResponse {
    constructor(
        corrId: number,
        private offset: Offset,
    ) {
        super(Commands.ConsumerUpdate, 1, corrId, RESPONSE_CODE_OK);
    }

    protected override build(): void {
        super.build();
        this.writeUInt16(this.offset.type);
        if (this.offset.type === OffsetTypes.Absolute) {
            this.writeUInt64(this.offset.value);
        } else if (this.offset.type === OffsetTypes.Timestamp) {
            this.writeInt64(BigInt(this.offset.value));
        }
    }
}
