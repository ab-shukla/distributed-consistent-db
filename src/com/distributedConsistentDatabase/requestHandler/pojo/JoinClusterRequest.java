package com.distributedConsistentDatabase.requestHandler.pojo;

import com.distributedConsistentDatabase.cluster.pojo.ClusterNode;

public class JoinClusterRequest {
    private ClusterNode node;

    public ClusterNode getNode() {
        return node;
    }

    public void setNode(final ClusterNode node) {
        this.node = node;
    }
}
