package com.distributedConsistentDatabase.dataStore;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryKeyValueStore implements KeyValueStore<String, String> {
    private static final InMemoryKeyValueStore keyValueStore = new InMemoryKeyValueStore();
    private final Map<String, String> inMemoryStore;
    private InMemoryKeyValueStore() {
        inMemoryStore = new ConcurrentHashMap<>();
    }

    public static InMemoryKeyValueStore getInstance() {
        return keyValueStore;
    }

    public String get(final String key) {
        return inMemoryStore.get(key);
    }

    public boolean put(final String key, final String value) {
        if (inMemoryStore.put(key, value) == value) {
            return false;
        } else {
            return true;
        }
    }

    public boolean delete(final String key) {
        if (inMemoryStore.remove(key) == null) {
            return false;
        } else {
            return true;
        }
    }
}
