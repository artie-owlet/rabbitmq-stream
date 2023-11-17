import { Commands } from './constants';
import { ClientRequest } from './client-message';
import { Offset, OffsetTypes } from './offset';

export class SubscribeRequest extends ClientRequest {
    constructor(
        private subscriptionId: number,
        private stream: string,
        private offset: Offset,
        private credit: number,
        private properties: Map<string, string>,
    ) {
        super(Commands.Subscribe, 1);
    }

    protected override build(corrId: number): void {
        super.build(corrId);
        this.writeUInt8(this.subscriptionId);
        this.writeString(this.stream);
        this.writeUInt16(this.offset.type);
        if (this.offset.type === OffsetTypes.Absolute) {
            this.writeUInt64(this.offset.value);
        } else if (this.offset.type === OffsetTypes.Timestamp) {
            this.writeInt64(BigInt(this.offset.value));
        }
        this.writeUInt16(this.credit);
        this.writeArraySize(this.properties.size);
        this.properties.forEach((value, key) => {
            this.writeString(key);
            this.writeString(value);
        });
    }
}
