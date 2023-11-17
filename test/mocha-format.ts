/* eslint @typescript-eslint/ban-types: ["error", { "types": { "Function": false } }] */
import { Suite } from 'mocha';

export const describeMethod = Object.assign(
    (method: Function, fn: (this: Suite) => void) => {
        return describe(`#${method.name}()`, fn);
    },
    {
        only: (method: Function, fn: (this: Suite) => void) => {
            return describe.only(`#${method.name}()`, fn);
        },
        skip: (method: Function, fn: (this: Suite) => void) => {
            return describe.skip(`#${method.name}()`, fn);
        },
    },
);
