export class MultiMap<K, V> {
    private map = new Map<K, Set<V>>();

    public add(key: K, value: V): void {
        this.map.get(key)?.add(value) ?? this.map.set(key, new Set([value]));
    }

    public delete(key: K, value: V): void {
        const values = this.map.get(key);
        if (values) {
            values.delete(value);
            if (values.size === 0) {
                this.map.delete(key);
            }
        }
    }

    public getAll(key: K): Set<V> | undefined {
        return this.map.get(key);
    }
}
