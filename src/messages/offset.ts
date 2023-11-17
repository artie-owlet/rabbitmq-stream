export enum OffsetTypes {
    None = 0,
    First = 1,
    Last = 2,
    Next = 3,
    Absolute = 4,
    Timestamp = 5,
}

export interface IRelativeOffset {
    type: OffsetTypes.None | OffsetTypes.First | OffsetTypes.Last | OffsetTypes.Next;
}

export interface IAbsoluteOffset {
    type: OffsetTypes.Absolute;
    value: bigint;
}

export interface ITimestampOffset {
    type: OffsetTypes.Timestamp;
    value: number;
}

export type Offset = IRelativeOffset | IAbsoluteOffset | ITimestampOffset;
