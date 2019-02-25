package com.distributedConsistentDatabase.cluster;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.distributedConsistentDatabase.cluster.pojo.ClusterNode;
import com.distributedConsistentDatabase.sao.DistributedConsistentDatabaseSAO;

/**
 * Stubbed SAO object to simulate service calls between different NodeManagers.
 * @author abshukla
 */
public class DistributedConsistentDatabaseSAOStub extends DistributedConsistentDatabaseSAO {

    private final Map<Integer, NodeManager> nodeIdToNodeManagerMap;
    public DistributedConsistentDatabaseSAOStub() {
        super(null);
        nodeIdToNodeManagerMap = new HashMap<>();
    }

    public void addNodeIdToNodeManagerMapping(final int nodeId, final NodeManager nodeManager) {
        nodeIdToNodeManagerMap.put(nodeId, nodeManager);
    }

    public void clear() {
        nodeIdToNodeManagerMap.clear();
    }

    @Override
    public boolean putValue(final ClusterNode node, final String key, final String value) {
        return nodeIdToNodeManagerMap.get(node.getNodeId()).putValueToCluster(key, value);
    }

    @Override
    public boolean internalPutValue(final ClusterNode node, final String key, final String value) {
        return nodeIdToNodeManagerMap.get(node.getNodeId()).putValue(key, value);
    }

    @Override
    public boolean deleteValue(final ClusterNode node, final String key) {
        return nodeIdToNodeManagerMap.get(node.getNodeId()).deleteValueFromCluster(key);
    }

    @Override
    public boolean internalDeleteValue(final ClusterNode node, final String key) {
        return nodeIdToNodeManagerMap.get(node.getNodeId()).delete(key);
    }

    @Override
    public String internalGetValue(final ClusterNode node, final String key) {
        return nodeIdToNodeManagerMap.get(node.getNodeId()).getValue(key);
    }

    @Override
    public boolean isHeartbeatSuccessfull(final ClusterNode node) {
        nodeIdToNodeManagerMap.get(node.getNodeId()).ping();
        return true;
    }

    @Override
    public boolean addClusterNode(final ClusterNode targetNode, final ClusterNode currentNode) {
        nodeIdToNodeManagerMap.get(targetNode.getNodeId()).addOtherClusterNode(currentNode);
        return false;
    }

    @Override
    public List<ClusterNode> getClusterDetails(final ClusterNode node) {
        return new ArrayList<>(nodeIdToNodeManagerMap.get(node.getNodeId()).getClusterDetails(null));
    }
}
