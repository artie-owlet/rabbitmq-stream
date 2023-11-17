import { Commands } from './messages/constants';

const responses = new Map([
    [0x01, 'OK'],
    [0x02, 'Stream does not exist'],
    [0x03, 'Subscription ID already exists'],
    [0x04, 'Subscription ID does not exist'],
    [0x05, 'Stream already exists'],
    [0x06, 'Stream not available'],
    [0x07, 'SASL mechanism not supported'],
    [0x08, 'Authentication failure'],
    [0x09, 'SASL error'],
    [0x0a, 'SASL challenge'],
    [0x0b, 'SASL authentication failure loopback'],
    [0x0c, 'Virtual host access failure'],
    [0x0d, 'Unknown frame'],
    [0x0e, 'Frame too large'],
    [0x0f, 'Internal error'],
    [0x10, 'Access refused'],
    [0x11, 'Precondition failed'],
    [0x12, 'Publisher does not exist'],
    [0x13, 'No offset'],
]);

export function printError(code: number): string {
    return responses.get(code) /* c8 ignore next */ || 'Unknown error';
}

export class StreamError extends Error {
    constructor(
        public readonly command: Commands,
        public readonly code: number,
    ) {
        super(`${Commands[command]} failed: ${printError(code)}, code=${code}`);
    }
}
