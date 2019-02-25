package com.distributedConsistentDatabase.dataStore;

import java.util.UUID;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class InMemoryKeyValueStoreTest {

    private KeyValueStore<String, String> keyValueStore;

    @Before
    public void setUp() throws Exception {
        keyValueStore = KeyValueStoreFactory.getKeyValueStore();
    }

    @After
    public void tearDown() throws Exception {
        keyValueStore.clear();
    }

    @Test
    public void testPutValue() {
        Assert.assertTrue(keyValueStore.put(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
    }

    @Test
    public void testRePutValue() {
        final String key = UUID.randomUUID().toString();
        final String value = UUID.randomUUID().toString();
        Assert.assertTrue(keyValueStore.put(key, value));
        Assert.assertFalse(keyValueStore.put(key, value));
    }

    @Test
    public void testGetValueAfterPut() {
        final String key = UUID.randomUUID().toString();
        final String value = UUID.randomUUID().toString();
        Assert.assertTrue(keyValueStore.put(key, value));
        Assert.assertEquals(keyValueStore.get(key), value);
    }

    @Test
    public void testGetValueWithoutPut() {
        Assert.assertNull(keyValueStore.get(UUID.randomUUID().toString()));
    }

    @Test
    public void testDeleteValueAfterPut() {
        final String key = UUID.randomUUID().toString();
        final String value = UUID.randomUUID().toString();
        Assert.assertTrue(keyValueStore.put(key, value));
        Assert.assertEquals(keyValueStore.get(key), value);
        Assert.assertTrue(keyValueStore.delete(key));
        Assert.assertNull(keyValueStore.get(key));
    }

    @Test
    public void testDeleteValueWithoutPut() {
        Assert.assertFalse(keyValueStore.delete(UUID.randomUUID().toString()));
    }
}
