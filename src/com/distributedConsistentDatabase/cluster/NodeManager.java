package com.distributedConsistentDatabase.cluster;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.distributedConsistentDatabase.cluster.pojo.ClusterNode;
import com.distributedConsistentDatabase.dataStore.KeyValueStore;
import com.distributedConsistentDatabase.dataStore.KeyValueStoreFactory;
import com.distributedConsistentDatabase.sao.DistributedConsistentDatabaseSAO;

public class NodeManager {
    private static final NodeManager INSTANCE = new NodeManager();
    private static boolean isInitialized = false;
    private ClusterNode currentNode;
    private ClusterManager clusterManager;
    private volatile boolean isLeader;
    private long lastTimestampPing;
    private KeyValueStore<String, String> keyValueStore;
    private DistributedConsistentDatabaseSAO dcdbSao;
    private boolean keyPutsInProgress;

    private NodeManager() {
        this.clusterManager = new ClusterManager();
        this.keyValueStore = KeyValueStoreFactory.getKeyValueStore();
        this.dcdbSao = new DistributedConsistentDatabaseSAO();
    }

    public static synchronized NodeManager of(final ClusterNode currentClusterNode) {
        if (false == isInitialized) {
            INSTANCE.currentNode = currentClusterNode;
            INSTANCE.lastTimestampPing = System.currentTimeMillis();
            INSTANCE.initialize();
            INSTANCE.clusterManager.addClusterNode(currentClusterNode);
            INSTANCE.keyPutsInProgress = false;
            INSTANCE.isLeader = true;
            isInitialized = true;
        }
        return INSTANCE;
    }

    public synchronized void addOtherClusterNode(final ClusterNode clusterNode) {
        // Check if the new node is to be the leader
        if (INSTANCE.isLeader) {
            if (INSTANCE.currentNode.getNodeId() > clusterNode.getNodeId()) {
                // we have a new leader which will init itself as a leader. change the current node to be a follower.
                INSTANCE.isLeader = false;
            }
        }
        INSTANCE.clusterManager.addClusterNode(clusterNode);
    }

    public ClusterNode getClusterLeader(final String key) {
        if (isLeader) {
            return INSTANCE.currentNode;
        } else {
            return INSTANCE.clusterManager.getClusterLeader();
        }
    }

    public List<ClusterNode> getClusterDetails(final String key) {
        return INSTANCE.clusterManager.getClusterNodes();
    }

    public void isPinged() {
        INSTANCE.lastTimestampPing = System.currentTimeMillis();
    }

    public void initialize() {
        final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if (isLeader) {
                    for (ClusterNode clusterNodeToPing : clusterManager.getClusterNodes()) {
                        if (false == isNodeHealthy(clusterNodeToPing)) {
                            clusterManager.removeClusterNode(clusterNodeToPing);
                        }
                    }
                } else if (System.currentTimeMillis() - lastTimestampPing > 10000L) {
                    // 10 seconds since the last ping. Assume the leader has died. remove leader from cluster.
                    clusterManager.removeClusterNode(clusterManager.getClusterLeader());
                    // Update the last ping time
                    lastTimestampPing = System.currentTimeMillis();
                    // Check if this node becomes the leader.
                    if (clusterManager.getClusterLeader().getNodeId() == INSTANCE.currentNode.getNodeId()) {
                        isLeader = true;
                    }
                }
            }

            private boolean isNodeHealthy(final ClusterNode node) {
                for (int i = 0; i < 3; i++) {
                    if (dcdbSao.isHeartbeatSuccessfull(node)) {
                        return true;
                    }
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException e1) {
                        //no-op
                    }
                }
                return false;
            }
        }, 0, 3000, TimeUnit.MILLISECONDS);
    }

    public String getValue(final String key) {
        return INSTANCE.keyValueStore.get(key);
    }

    public boolean putValue(final String key, final String value) {
        return INSTANCE.keyValueStore.put(key, value);
    }

    public boolean delete(final String key) {
        return INSTANCE.keyValueStore.delete(key);
    }

    public String getValueFromCluster(final String key) {
        final Map<String, Long> countingMap = INSTANCE.clusterManager.getClusterNodes().stream()
            .map(k -> INSTANCE.dcdbSao.internalGetValue(k, key))
            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        long maxOccurance = 0;
        String maxEntry = null;
        for (Entry<String, Long> entry : countingMap.entrySet()) {
            if (entry != null && entry.getValue() > maxOccurance) {
                maxEntry = entry.getKey();
            }
        }
        if (INSTANCE.clusterManager.getClusterQuorumSize() <= countingMap.get(maxEntry)) {
            return maxEntry;
        } else {
            throw new IllegalStateException();
        }
    }

    public synchronized boolean putValueToCluster(final String key, final String value) {
        // Leader puts the value to cluster
        if (INSTANCE.isLeader) {
            if (INSTANCE.keyPutsInProgress) {
                throw new IllegalStateException("key put in progress");
            }

            INSTANCE.keyPutsInProgress = true;
            final boolean result = putValue(key, value);
            // added to the current node.. so starting with success count as 1.
            int successCount = 1;
            try {
                for (final ClusterNode node : INSTANCE.clusterManager.getClusterNodes()) {
                    try {
                        INSTANCE.dcdbSao.internalPutValue(node, key, value);
                        successCount++;
                    } catch (final Exception e) {
                        // continue to the next node. no-op
                    }
                }
            } finally {
                INSTANCE.keyPutsInProgress = false;
            }

            if (INSTANCE.clusterManager.getClusterQuorumSize() <= successCount) {
                return result;
            } else {
                throw new IllegalStateException("quorum not met. quorum size: " + INSTANCE.clusterManager.getClusterQuorumSize()
                   + ". success count: " + successCount);
            }
        } else {
            // follower just redirects the request to leader.
            return INSTANCE.dcdbSao.putValue(INSTANCE.getClusterLeader(null), key, value);
        }
    }
}
