package com.distributedConsistentDatabase.dataStore;

/**
 * Interface for the key-value store. The interface supports extension for any key type and value type.
 * Individual implementations to provide additional constraints on the key and value space.
 * @author abshukla
 *
 * @param <K> : Key type
 * @param <V> : Value type
 */
public interface KeyValueStore<K, V> {

    /**
     * Method the get the value corresponding to any key. The implementation must ensure that if key is not
     * present, null should be returned in the response.
     * @param key : key to lookup
     * @return : value corresponding to the key if present, null otherwise.
     */
    public V get(final K key);

    /**
     * Method to put/ update a key value pair in the store.
     * @param key : key to insert
     * @param value : value corresponding to the key
     * @return : true of the value is added/ updated. false, if the value was not updated or was already available.
     */
    public boolean put(final K key, final V value);

    /**
     * Method to delete entry of a specific key value set.
     * @param key : key to delete from store
     * @return : true of the pair was deleted, false otherwise.
     */
    public boolean delete(final K key);

    /**
     * Clears all the entries.
     */
    public void clear();
}
