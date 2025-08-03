package concurrency.threadpools.CustomMaps;

import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.Objects;
import java.util.function.BiFunction;

public class LockFreeHashMap<K, V> {
    private final AtomicReferenceArray<Node<K, V>> buckets;
    private final int mask;

    public LockFreeHashMap(int capacity) {
        int cap = 1;
        while (cap < capacity) cap <<= 1;
        this.mask = cap - 1;
        this.buckets = new AtomicReferenceArray<>(cap);
    }

    public V get(K key) {
        Node<K, V> head = buckets.get(index(key));
        while (head != null) {
            if (Objects.equals(head.key, key)) return head.value;
            head = head.next;
        }
        return null;
    }

    public void put(K key, V value) {
        int idx = index(key);
        while (true) {
            Node<K, V> oldHead = buckets.get(idx);
            Node<K, V> newHead = new Node<>(key, value, oldHead);
            if (buckets.compareAndSet(idx, oldHead, newHead)) return;
        }
    }

    public void compute(K key, BiFunction<K, V, V> fn) {
        int idx = index(key);
        while (true) {
            Node<K, V> oldHead = buckets.get(idx);
            V oldVal = null;
            Node<K, V> cur = oldHead;
            while (cur != null) {
                if (Objects.equals(cur.key, key)) {
                    oldVal = cur.value;
                    break;
                }
                cur = cur.next;
            }
            V newVal = fn.apply(key, oldVal);
            Node<K, V> newHead = buildUpdatedList(oldHead, key, newVal);
            if (buckets.compareAndSet(idx, oldHead, newHead)) return;
        }
    }

    private Node<K, V> buildUpdatedList(Node<K, V> head, K key, V newVal) {
        if (head == null) return (newVal == null) ? null : new Node<>(key, newVal, null);
        if (Objects.equals(head.key, key)) {
            return (newVal == null) ? head.next : new Node<>(key, newVal, head.next);
        }
        Node<K, V> next = buildUpdatedList(head.next, key, newVal);
        if (next == head.next) return head;
        return new Node<>(head.key, head.value, next);
    }

    private int index(K key) {
        return (key.hashCode() & 0x7fffffff) & mask;
    }

    static class Node<K, V> {
        final K key;
        final V value;
        final Node<K, V> next;

        Node(K key, V value, Node<K, V> next) {
            this.key = key;
            this.value = value;
            this.next = next;
        }
    }
}
