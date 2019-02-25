package com.distributedConsistentDatabase.dataStore;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In memory key value store implementation. Class allows only a single instance to be created.
 * @author abshukla
 */
public class InMemoryKeyValueStore implements KeyValueStore<String, String> {
    private final Map<String, String> inMemoryStore;

    /**
     * Private constructor to ensure a single instance per node.
     */
    public InMemoryKeyValueStore() {
        inMemoryStore = new ConcurrentHashMap<>();
    }

    /**
     * Method to get the singleton instance.
     * @return : singleton keyValueStore instance
     */
    public static InMemoryKeyValueStore getInstance() {
        return new InMemoryKeyValueStore();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String get(final String key) {
        return inMemoryStore.get(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean put(final String key, final String value) {
        if (inMemoryStore.put(key, value) == value) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean delete(final String key) {
        if (inMemoryStore.remove(key) == null) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear() {
        inMemoryStore.clear();
    }
}
