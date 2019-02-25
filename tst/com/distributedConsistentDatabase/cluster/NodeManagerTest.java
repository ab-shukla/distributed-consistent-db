package com.distributedConsistentDatabase.cluster;

import java.util.UUID;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.distributedConsistentDatabase.cluster.pojo.ClusterNode;

/**
 * This test class simulates the cluster structure by initializing multiple node managers
 * and communicating within them via the SAO stub.
 * @author abshukla
 */
public class NodeManagerTest {

    private NodeManager nodeManager;
    private DistributedConsistentDatabaseSAOStub saoStub;

    @Before
    public void setUp() throws Exception {
        saoStub = new DistributedConsistentDatabaseSAOStub();
        nodeManager = new NodeManager(saoStub);
    }

    @After
    public void tearDown() throws Exception {
        saoStub.clear();
    }

    @Test
    public void testInitializeLeaderNode() {
        final ClusterNode currentNode = new ClusterNode();
        currentNode.setNodeId(1);
        saoStub.addNodeIdToNodeManagerMapping(1, nodeManager);
        nodeManager.initialize(currentNode, null);
        // Nothing to assert here as the leader does not have anyone to ping
    }

    @Test
    public void testInitializeLeaderFollowedByFollowerNode() {
        final ClusterNode leaderNode = new ClusterNode();
        leaderNode.setNodeId(1);
        saoStub.addNodeIdToNodeManagerMapping(1, nodeManager);
        nodeManager.initialize(leaderNode, null);

        final NodeManager followerNodeManager = new NodeManager(saoStub);
        final ClusterNode followerNode = new ClusterNode();
        followerNode.setNodeId(2);
        saoStub.addNodeIdToNodeManagerMapping(2, followerNodeManager);
        followerNodeManager.initialize(followerNode, leaderNode);

        long currentPingMillis = followerNodeManager.getLastPingTimestampMillis();
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            // no-op
        }

        Assert.assertTrue(currentPingMillis < followerNodeManager.getLastPingTimestampMillis());
    }

    @Test
    public void testInitializeFollowerFollowedByLeaderNode() {
        final NodeManager followerNodeManager = new NodeManager(saoStub);
        final ClusterNode followerNode = new ClusterNode();
        followerNode.setNodeId(2);
        saoStub.addNodeIdToNodeManagerMapping(2, followerNodeManager);
        followerNodeManager.initialize(followerNode, null);

        final ClusterNode leaderNode = new ClusterNode();
        leaderNode.setNodeId(1);
        saoStub.addNodeIdToNodeManagerMapping(1, nodeManager);
        nodeManager.initialize(leaderNode, followerNode);

        long currentPingMillis = followerNodeManager.getLastPingTimestampMillis();
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            // no-op
        }

        Assert.assertTrue(currentPingMillis < followerNodeManager.getLastPingTimestampMillis());
    }

    @Test
    public void testQuorumMetForPutOnFollower() {
        final NodeManager firstFollowerNodeManager = new NodeManager(saoStub);
        final ClusterNode firstFollowerNode = new ClusterNode();
        firstFollowerNode.setNodeId(2);
        saoStub.addNodeIdToNodeManagerMapping(2, firstFollowerNodeManager);
        firstFollowerNodeManager.initialize(firstFollowerNode, null);

        final ClusterNode leaderNode = new ClusterNode();
        leaderNode.setNodeId(1);
        saoStub.addNodeIdToNodeManagerMapping(1, nodeManager);
        nodeManager.initialize(leaderNode, firstFollowerNode);

        final NodeManager secondFollowerNodeManager = new NodeManager(saoStub);
        final ClusterNode secondFollowerNode = new ClusterNode();
        secondFollowerNode.setNodeId(3);
        saoStub.addNodeIdToNodeManagerMapping(3, secondFollowerNodeManager);
        secondFollowerNodeManager.initialize(secondFollowerNode, firstFollowerNode);

        final String key = UUID.randomUUID().toString();
        final String value = UUID.randomUUID().toString();
        Assert.assertTrue(secondFollowerNodeManager.putValueToCluster(key, value));
    }

    @Test(expected = IllegalStateException.class)
    public void testQuorumNotMetForPutOnLeader() {
        final NodeManager firstFollowerNodeManager = new NodeManager(saoStub);
        final ClusterNode firstFollowerNode = new ClusterNode();
        firstFollowerNode.setNodeId(2);
        saoStub.addNodeIdToNodeManagerMapping(2, firstFollowerNodeManager);
        firstFollowerNodeManager.initialize(firstFollowerNode, null);

        final ClusterNode leaderNode = new ClusterNode();
        leaderNode.setNodeId(1);
        saoStub.addNodeIdToNodeManagerMapping(1, nodeManager);
        nodeManager.initialize(leaderNode, firstFollowerNode);

        final String key = UUID.randomUUID().toString();
        final String value = UUID.randomUUID().toString();
        nodeManager.putValueToCluster(key, value);
    }

    @Test
    public void testQuorumMetForGetOnFollower() {
        final NodeManager firstFollowerNodeManager = new NodeManager(saoStub);
        final ClusterNode firstFollowerNode = new ClusterNode();
        firstFollowerNode.setNodeId(2);
        saoStub.addNodeIdToNodeManagerMapping(2, firstFollowerNodeManager);
        firstFollowerNodeManager.initialize(firstFollowerNode, null);

        final ClusterNode leaderNode = new ClusterNode();
        leaderNode.setNodeId(1);
        saoStub.addNodeIdToNodeManagerMapping(1, nodeManager);
        nodeManager.initialize(leaderNode, firstFollowerNode);

        final NodeManager secondFollowerNodeManager = new NodeManager(saoStub);
        final ClusterNode secondFollowerNode = new ClusterNode();
        secondFollowerNode.setNodeId(3);
        saoStub.addNodeIdToNodeManagerMapping(3, secondFollowerNodeManager);
        secondFollowerNodeManager.initialize(secondFollowerNode, firstFollowerNode);

        final String key = UUID.randomUUID().toString();
        final String value = UUID.randomUUID().toString();

        // setup all nodes to contain the same key value pair so that quorum can be met.
        nodeManager.putValue(key, value);
        firstFollowerNodeManager.putValue(key, value);
        secondFollowerNodeManager.putValue(key, value);
        Assert.assertEquals(secondFollowerNodeManager.getValueFromCluster(key), value);
    }

    @Test(expected = IllegalStateException.class)
    public void testQuorumNotMetForGetOnLeader() {
        final NodeManager firstFollowerNodeManager = new NodeManager(saoStub);
        final ClusterNode firstFollowerNode = new ClusterNode();
        firstFollowerNode.setNodeId(2);
        saoStub.addNodeIdToNodeManagerMapping(2, firstFollowerNodeManager);
        firstFollowerNodeManager.initialize(firstFollowerNode, null);

        final ClusterNode leaderNode = new ClusterNode();
        leaderNode.setNodeId(1);
        saoStub.addNodeIdToNodeManagerMapping(1, nodeManager);
        nodeManager.initialize(leaderNode, firstFollowerNode);

        final NodeManager secondFollowerNodeManager = new NodeManager(saoStub);
        final ClusterNode secondFollowerNode = new ClusterNode();
        secondFollowerNode.setNodeId(3);
        saoStub.addNodeIdToNodeManagerMapping(3, secondFollowerNodeManager);
        secondFollowerNodeManager.initialize(secondFollowerNode, firstFollowerNode);

        final String key = UUID.randomUUID().toString();
        final String value = UUID.randomUUID().toString();
        final String garbageValue = UUID.randomUUID().toString();

        // setup all nodes to contain the same key value pair except 1 node to ensure that quorom is not met.
        nodeManager.putValue(key, value);
        firstFollowerNodeManager.putValue(key, value);
        secondFollowerNodeManager.putValue(key, garbageValue);
        nodeManager.getValueFromCluster(key);
    }

    @Test
    public void testQuorumMetForDeleteOnFollower() {
        final NodeManager firstFollowerNodeManager = new NodeManager(saoStub);
        final ClusterNode firstFollowerNode = new ClusterNode();
        firstFollowerNode.setNodeId(2);
        saoStub.addNodeIdToNodeManagerMapping(2, firstFollowerNodeManager);
        firstFollowerNodeManager.initialize(firstFollowerNode, null);

        final ClusterNode leaderNode = new ClusterNode();
        leaderNode.setNodeId(1);
        saoStub.addNodeIdToNodeManagerMapping(1, nodeManager);
        nodeManager.initialize(leaderNode, firstFollowerNode);

        final NodeManager secondFollowerNodeManager = new NodeManager(saoStub);
        final ClusterNode secondFollowerNode = new ClusterNode();
        secondFollowerNode.setNodeId(3);
        saoStub.addNodeIdToNodeManagerMapping(3, secondFollowerNodeManager);
        secondFollowerNodeManager.initialize(secondFollowerNode, firstFollowerNode);

        final String key = UUID.randomUUID().toString();
        final String value = UUID.randomUUID().toString();

        // setup all nodes to contain the same key value pair so that quorum can be met.
        nodeManager.putValue(key, value);
        firstFollowerNodeManager.putValue(key, value);
        secondFollowerNodeManager.putValue(key, value);
        Assert.assertTrue(secondFollowerNodeManager.deleteValueFromCluster(key));
    }

    @Test(expected = IllegalStateException.class)
    public void testQuorumNotMetForDeleteOnLeader() {
        final NodeManager firstFollowerNodeManager = new NodeManager(saoStub);
        final ClusterNode firstFollowerNode = new ClusterNode();
        firstFollowerNode.setNodeId(2);
        saoStub.addNodeIdToNodeManagerMapping(2, firstFollowerNodeManager);
        firstFollowerNodeManager.initialize(firstFollowerNode, null);

        final ClusterNode leaderNode = new ClusterNode();
        leaderNode.setNodeId(1);
        saoStub.addNodeIdToNodeManagerMapping(1, nodeManager);
        nodeManager.initialize(leaderNode, firstFollowerNode);

        final String key = UUID.randomUUID().toString();
        final String value = UUID.randomUUID().toString();

        // setup all nodes to contain the same key value pair except 1 node to ensure that quorom is not met.
        nodeManager.putValue(key, value);
        firstFollowerNodeManager.putValue(key, value);
        nodeManager.deleteValueFromCluster(key);
    }
}
