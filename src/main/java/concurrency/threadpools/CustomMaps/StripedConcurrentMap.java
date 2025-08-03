package concurrency.threadpools.CustomMaps;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;

public class StripedConcurrentMap<K, V> {
    private final Segment<K, V>[] segments;
    private final int mask;

    static final class Segment<K, V> {
        final ConcurrentHashMap<K, V> map = new ConcurrentHashMap<>();
        final ReentrantLock lock = new ReentrantLock();
    }

    @SuppressWarnings("unchecked")
    public StripedConcurrentMap(int stripes) {
        int cap = 1;
        while (cap < stripes) cap <<= 1;
        this.mask = cap - 1;
        this.segments = new Segment[cap];
        for (int i = 0; i < cap; i++) {
            segments[i] = new Segment<>();
        }
    }

    private Segment<K, V> segment(Object key) {
        return segments[(key.hashCode() & 0x7fffffff) & mask];
    }

    public V get(K key) {
        return segment(key).map.get(key);
    }

    public void put(K key, V value) {
        segment(key).map.put(key, value);
    }

    public void compute(K key, BiFunction<K, V, V> remappingFunction) {
        Segment<K, V> seg = segment(key);
        seg.lock.lock();
        try {
            seg.map.compute(key, remappingFunction);
        } finally {
            seg.lock.unlock();
        }
    }
}
