import { Context } from 'mocha';

export function testName(ctx: Context): string {
    return ctx.currentTest?.title.replaceAll(' ', '_') || '';
}
