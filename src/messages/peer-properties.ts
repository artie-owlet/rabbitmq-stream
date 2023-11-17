import process from 'process';

import { Commands } from './constants';
import { ClientRequest } from './client-message';
import { ServerResponse } from './server-message';

interface IPackage {
    name: string;
    version: string;
}

// eslint-disable-next-line @typescript-eslint/no-var-requires
const pkg = require('../../package.json') as IPackage;

export class PeerPropertiesRequest extends ClientRequest {
    private props = new Map([
        ['product', pkg.name],
        ['version', pkg.version],
        ['platform', `Node.js ${process.version}`],
    ]);

    constructor(connectionName?: string) {
        super(Commands.PeerProperties, 1);
        if (connectionName !== undefined) {
            this.props.set('connection_name', connectionName);
        }
    }

    protected override build(corrId: number): void {
        super.build(corrId);
        this.writeArraySize(this.props.size);
        this.props.forEach((value, key) => {
            this.writeString(key);
            this.writeString(value);
        });
    }
}

export class PeerPropertiesResponse extends ServerResponse {
    public properties = new Map<string, string>();

    constructor(msg: Buffer) {
        super(msg);
        const size = this.readArraySize();
        for (let i = 0; i < size; ++i) {
            this.properties.set(this.readString(), this.readString());
        }
    }
}
