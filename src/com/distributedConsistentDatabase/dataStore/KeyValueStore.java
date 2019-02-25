package com.distributedConsistentDatabase.dataStore;

public interface KeyValueStore<K, V> {
    public V get(final K key);
    public boolean put(final K key, final V value);
    public boolean delete(final K key);
}
