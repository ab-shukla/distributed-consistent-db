package com.distributedConsistentDatabase.requestHandler;

import java.util.Arrays;
import java.util.UUID;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.distributedConsistentDatabase.cluster.NodeManager;
import com.distributedConsistentDatabase.cluster.pojo.ClusterNode;
import com.distributedConsistentDatabase.requestHandler.pojo.BootstrapRequest;
import com.distributedConsistentDatabase.requestHandler.pojo.GetClusterResponse;
import com.distributedConsistentDatabase.requestHandler.pojo.JoinClusterRequest;
import com.distributedConsistentDatabase.requestHandler.pojo.KeyValueDetails;
import com.distributedConsistentDatabase.requestHandler.pojo.KeyValuePutRequest;

@RunWith(PowerMockRunner.class)
@PrepareForTest(DistributedConsistentDatabaseService.class)
public class DistributedConsistentDatabaseServiceTest {

    private DistributedConsistentDatabaseService service;
    private NodeManager mockNodeManager = EasyMock.createMock(NodeManager.class);

    @Before
    public void setUp() throws Exception {
        resetMocks();
        PowerMockito.whenNew(NodeManager.class).withAnyArguments().thenReturn(mockNodeManager);
        service = new DistributedConsistentDatabaseService();
    }

    @After
    public void tearDown() throws Exception {
        verifyMocks();
    }

    @Test
    public void testJoinCluster_Success() {
        final ClusterNode node = new ClusterNode();
        final JoinClusterRequest joinClusterRequest = new JoinClusterRequest();
        joinClusterRequest.setNode(node);
        this.mockNodeManager.addOtherClusterNode(node);
        EasyMock.expectLastCall();
        replayMocks();
        Assert.assertEquals(service.addClusterNode(joinClusterRequest).getStatus(), Status.OK.getStatusCode());
    }

    @Test
    public void testJoinCluster_Failure() {
        final ClusterNode node = new ClusterNode();
        final JoinClusterRequest joinClusterRequest = new JoinClusterRequest();
        joinClusterRequest.setNode(node);
        this.mockNodeManager.addOtherClusterNode(node);
        EasyMock.expectLastCall().andThrow(new IllegalStateException());
        replayMocks();
        Assert.assertEquals(service.addClusterNode(joinClusterRequest).getStatus(), Status.SERVICE_UNAVAILABLE.getStatusCode());
    }

    @Test
    public void testDelete_Success() {
        final String key = UUID.randomUUID().toString();
        EasyMock.expect(this.mockNodeManager.deleteValueFromCluster(key)).andReturn(true);
        replayMocks();
        Assert.assertEquals(service.deleteValue(key).getStatus(), Status.OK.getStatusCode());
    }

    @Test
    public void testDelete_Failure() {
        final String key = UUID.randomUUID().toString();
        EasyMock.expect(this.mockNodeManager.deleteValueFromCluster(key)).andThrow(new IllegalStateException());
        replayMocks();
        Assert.assertEquals(service.deleteValue(key).getStatus(), Status.SERVICE_UNAVAILABLE.getStatusCode());
    }

    @Test
    public void testGetClusterDetails_Success() {
        final ClusterNode node = new ClusterNode();
        EasyMock.expect(this.mockNodeManager.getClusterDetails(null)).andReturn(Arrays.asList(node));
        replayMocks();

        final GetClusterResponse response = service.getClusterDetails();
        Assert.assertNotNull(response);
        Assert.assertTrue(response.getServerList().size() == 1);
        Assert.assertEquals(response.getServerList().get(0), node);
    }

    @Test
    public void testBootstrap_Success() {
        final ClusterNode currentNode = new ClusterNode();
        currentNode.setIp(UUID.randomUUID().toString());
        currentNode.setNodeId(1);
        currentNode.setPort(UUID.randomUUID().toString());

        final ClusterNode seedServer = new ClusterNode();
        final BootstrapRequest request = new BootstrapRequest();
        request.setSeedServer(seedServer);
        request.setIp(currentNode.getIp());
        request.setNodeId(String.valueOf(currentNode.getNodeId()));
        request.setPort(currentNode.getPort());
        this.mockNodeManager.initialize(currentNode, seedServer);
        EasyMock.expectLastCall();
        replayMocks();

        Assert.assertEquals(service.bootstrap(request).getStatus(), Status.OK.getStatusCode());
    }

    @Test
    public void testHeartbeat_Success() {
        this.mockNodeManager.ping();
        EasyMock.expectLastCall();
        replayMocks();

        Assert.assertEquals(service.getHeartbeat().getStatus(), Status.OK.getStatusCode());
    }

    @Test
    public void testGetValue_Success() {
        final String key = UUID.randomUUID().toString();
        final String value = UUID.randomUUID().toString();
        EasyMock.expect(this.mockNodeManager.getValueFromCluster(key)).andReturn(value);
        replayMocks();

        final Response response = service.getValue(key);
        Assert.assertEquals(response.getStatus(), Status.OK.getStatusCode());
        Assert.assertEquals(response.getEntity(), value);
    }

    @Test
    public void testGetValueInternal_Success() {
        final String key = UUID.randomUUID().toString();
        final String value = UUID.randomUUID().toString();
        EasyMock.expect(this.mockNodeManager.getValue(key)).andReturn(value);
        replayMocks();

        final Response response = service.internalGetValue(key);
        Assert.assertEquals(response.getStatus(), Status.OK.getStatusCode());
        Assert.assertEquals(response.getEntity(), value);
    }

    @Test
    public void testPutValue_Success() {
        final String key = UUID.randomUUID().toString();
        final String value = UUID.randomUUID().toString();
        final KeyValueDetails details = new KeyValueDetails();
        details.setKey(key);
        details.setValue(value);
        final KeyValuePutRequest request = new KeyValuePutRequest();
        request.setRequest(details);
        EasyMock.expect(this.mockNodeManager.putValueToCluster(key, value)).andReturn(true);
        replayMocks();

        Assert.assertEquals(service.putValue(request).getStatus(), Status.OK.getStatusCode());
    }

    @Test
    public void testPutValue_Failure() {
        final String key = UUID.randomUUID().toString();
        final String value = UUID.randomUUID().toString();
        final KeyValueDetails details = new KeyValueDetails();
        details.setKey(key);
        details.setValue(value);
        final KeyValuePutRequest request = new KeyValuePutRequest();
        request.setRequest(details);
        EasyMock.expect(this.mockNodeManager.putValueToCluster(key, value)).andThrow(new IllegalStateException());
        replayMocks();

        Assert.assertEquals(service.putValue(request).getStatus(), Status.SERVICE_UNAVAILABLE.getStatusCode());
    }

    @Test
    public void testPutValueInternal_Success() {
        final String key = UUID.randomUUID().toString();
        final String value = UUID.randomUUID().toString();
        final KeyValueDetails details = new KeyValueDetails();
        details.setKey(key);
        details.setValue(value);
        final KeyValuePutRequest request = new KeyValuePutRequest();
        request.setRequest(details);
        EasyMock.expect(this.mockNodeManager.putValue(key, value)).andReturn(true);
        replayMocks();

        Assert.assertEquals(service.internalPutValue(request).getStatus(), Status.OK.getStatusCode());
    }

    @Test
    public void testPutValueInternal_Failure() {
        final String key = UUID.randomUUID().toString();
        final String value = UUID.randomUUID().toString();
        final KeyValueDetails details = new KeyValueDetails();
        details.setKey(key);
        details.setValue(value);
        final KeyValuePutRequest request = new KeyValuePutRequest();
        request.setRequest(details);

        EasyMock.expect(this.mockNodeManager.putValue(key, value)).andThrow(new IllegalStateException());
        replayMocks();

        Assert.assertEquals(service.internalPutValue(request).getStatus(), Status.SERVICE_UNAVAILABLE.getStatusCode());
    }

    private void resetMocks() {
        EasyMock.reset(mockNodeManager);
    }

    private void replayMocks() {
        EasyMock.replay(mockNodeManager);
    }

    private void verifyMocks() {
        EasyMock.verify(mockNodeManager);
    }
}
