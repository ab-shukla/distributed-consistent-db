package com.distributedConsistentDatabase.dataStore;

public class KeyValueStoreFactory {
    public static KeyValueStore<String, String> getKeyValueStore() {
        return InMemoryKeyValueStore.getInstance();
    }
}
