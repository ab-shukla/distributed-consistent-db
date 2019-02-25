package com.distributedConsistentDatabase.cluster;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.distributedConsistentDatabase.cluster.pojo.ClusterNode;

public class ClusterManager {
    private static final int HEALTHY_CLUSTER_MIN_SIZE = 3;
    private List<ClusterNode> clusterNodes;

    public ClusterManager() {
        this.clusterNodes = new ArrayList<>();
    }

    public ClusterManager(final List<ClusterNode> clusterNodes) {
        this.clusterNodes = clusterNodes;
    }

    public void addClusterNode(final ClusterNode node) {
        this.clusterNodes.add(node);
    }

    public void removeClusterNode(final ClusterNode node) {
        this.clusterNodes.remove(node);
    }

    public List<ClusterNode> getClusterNodes() {
        return clusterNodes;
    }

    public ClusterNode getClusterLeader() {
        return clusterNodes.stream().min(Comparator.comparing(ClusterNode::getNodeId)).get();
    }

    public boolean isClusterHealthy() {
        return (clusterNodes.size() >= HEALTHY_CLUSTER_MIN_SIZE);
    }

    public int getClusterQuorumSize() {
        final int currentClusterQuorum = (clusterNodes.size()/ 2) + 1;
        return Math.max(currentClusterQuorum, HEALTHY_CLUSTER_MIN_SIZE);
    }
}
