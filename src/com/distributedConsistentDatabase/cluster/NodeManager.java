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

/**
 * Node manager class which is responsible for managing all cluster actions from the current node. The class is
 * responsible for performing actions based on whether it is the leader or the follower node. The class has a constructor
 * but has to be initialized with an initialize method. This class holds the details of complete cluster and the key-value
 * store pointer through composition. All the servers initialize thinking of themselves as leaders, and with the data from
 * seed servers, they fall back to the Follower position, if applicable.
 * @author abshukla
 */
public class NodeManager {
    private boolean isInitialized;
    private volatile boolean isLeader;
    private ClusterNode currentNode;
    private ClusterManager clusterManager;
    private long lastPingTimestampMillis;  // represents the last time this node was pinged by anyone.
    private KeyValueStore<String, String> keyValueStore;
    private DistributedConsistentDatabaseSAO dcdbSao;

    /**
     * Constructor
     */
    public NodeManager(final DistributedConsistentDatabaseSAO distributedConsistentDatabaseSAO) {
        this.clusterManager = new ClusterManager();
        this.keyValueStore = KeyValueStoreFactory.getKeyValueStore();
        this.dcdbSao = distributedConsistentDatabaseSAO;
        this.isInitialized = false;
    }

    /**
     * Initializes the current node with the seed server. The method also kicks off the thread that is responsible for the
     * scheduled node operations (like pinging other nodes). It uses the data from the seed server and joins itself to all
     * the nodes.
     * @param currentClusterNode
     * @param seedServerNode
     */
    public synchronized void initialize(final ClusterNode currentClusterNode, final ClusterNode seedServerNode) {
        if (false == isInitialized) {
            this.currentNode = currentClusterNode;
            this.lastPingTimestampMillis = System.currentTimeMillis();
            this.initialize();
            this.clusterManager.addClusterNode(currentClusterNode);
            this.isLeader = true;

            if (seedServerNode != null) {
                final List<ClusterNode> clusterNodeList =
                    dcdbSao.getClusterDetails(seedServerNode);
                // Add the current node to all cluster nodes
                for (final ClusterNode node : clusterNodeList) {
                    dcdbSao.addClusterNode(node, currentNode);
                    addOtherClusterNode(node);
                }
            }

            isInitialized = true;
        }
    }

    /**
     * Adds an incoming node to the nodemanager and its clusterList. If current node is leader and the incoming node has
     * lower node id then the current node forfeits its Leader position. Since the other node would have also started
     * as Leader, it takes the position of the leader.
     * @param clusterNode : cluster node to add.
     */
    public synchronized void addOtherClusterNode(final ClusterNode clusterNode) {
        // Check if the new node is to be the leader
        if (this.isLeader) {
            if (this.currentNode.getNodeId() > clusterNode.getNodeId()) {
                // we have a new leader which will init itself as a leader. change the current node to be a follower.
                this.isLeader = false;
            }
        }
        this.clusterManager.addClusterNode(clusterNode);
    }

    /**
     * Returns the cluster leader for the provided key.
     * TODO: Implement ClusterMesh where multiple clusters can exist with individual key spaces (and implement data partioning)
     * @param key : key for which the cluster leader is required.
     * @return : Leader cluster node
     */
    public ClusterNode getClusterLeader(final String key) {
        if (isLeader) {
            return this.currentNode;
        } else {
            return this.clusterManager.getClusterLeader();
        }
    }

    /**
     * Fetches the list of nodes in the cluster for the current node.
     * @param key : key for which the cluster nodes are required.
     * @return : node list representing the cluster for the given key space.
     */
    public List<ClusterNode> getClusterDetails(final String key) {
        return this.clusterManager.getClusterNodes();
    }

    /**
     * Method to ping the current node.
     */
    public void ping() {
        this.lastPingTimestampMillis = System.currentTimeMillis();
    }

    private void initialize() {
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
                } else if (System.currentTimeMillis() - lastPingTimestampMillis > 10000L) {
                    // 10 seconds since the last ping. Assume the leader has died. remove leader from cluster.
                    clusterManager.removeClusterNode(clusterManager.getClusterLeader());
                    // Update the last ping time
                    lastPingTimestampMillis = System.currentTimeMillis();
                    // Check if this node becomes the leader.
                    if (clusterManager.getClusterLeader().getNodeId() == currentNode.getNodeId()) {
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
        return this.keyValueStore.get(key);
    }

    public boolean putValue(final String key, final String value) {
        return this.keyValueStore.put(key, value);
    }

    public boolean delete(final String key) {
        return this.keyValueStore.delete(key);
    }

    public String getValueFromCluster(final String key) {
        final Map<String, Long> countingMap = this.clusterManager.getClusterNodes().stream()
            .map(k -> this.dcdbSao.internalGetValue(k, key))
            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        long maxOccurance = 0;
        String maxEntry = null;
        for (Entry<String, Long> entry : countingMap.entrySet()) {
            if (entry != null && entry.getValue() > maxOccurance) {
                maxEntry = entry.getKey();
            }
        }

        if (this.clusterManager.getClusterQuorumSize() <= countingMap.get(maxEntry)) {
            return maxEntry;
        } else {
            throw new IllegalStateException();
        }
    }

    public synchronized boolean putValueToCluster(final String key, final String value) {
        // Leader puts the value to cluster
        if (this.isLeader) {
            final boolean result = putValue(key, value);
            // added to the current node.. so starting with success count as 1.
            int successCount = 1;
            for (final ClusterNode node : this.clusterManager.getClusterNodes()) {
                if (node.getNodeId() == this.currentNode.getNodeId()) {
                    // we have already put entry to this store. skipping
                    continue;
                }

                try {
                    this.dcdbSao.internalPutValue(node, key, value);
                    successCount++;
                } catch (final Exception e) {
                    // continue to the next node. no-op
                }
            }

            if (this.clusterManager.getClusterQuorumSize() <= successCount) {
                return result;
            } else {
                throw new IllegalStateException("quorum not met. quorum size: " + this.clusterManager.getClusterQuorumSize()
                   + ". success count: " + successCount);
            }
        } else {
            // follower just redirects the request to leader.
            return this.dcdbSao.putValue(this.getClusterLeader(null), key, value);
        }
    }

    public synchronized boolean deleteValueFromCluster(final String key) {
        // Leader puts the value to cluster
        if (this.isLeader) {
            final boolean result = delete(key);
            // deleted from current node.. so starting with success count as 1.
            int successCount = 1;
            for (final ClusterNode node : this.clusterManager.getClusterNodes()) {
                if (node.getNodeId() == this.currentNode.getNodeId()) {
                    // we have already removed entry from this store. skipping
                    continue;
                }

                try {
                    this.dcdbSao.internalDeleteValue(node, key);
                    successCount++;
                } catch (final Exception e) {
                    // continue to the next node. no-op
                }
            }

            if (this.clusterManager.getClusterQuorumSize() <= successCount) {
                return result;
            } else {
                throw new IllegalStateException("quorum not met. quorum size: " + this.clusterManager.getClusterQuorumSize()
                   + ". success count: " + successCount);
            }
        } else {
            // follower just redirects the request to leader.
            return this.dcdbSao.deleteValue(this.getClusterLeader(null), key);
        }
    }

    public long getLastPingTimestampMillis() {
        return this.lastPingTimestampMillis;
    }
}
