package com.distributedConsistentDatabase.sao;

import java.util.UUID;

import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.distributedConsistentDatabase.cluster.pojo.ClusterNode;
import com.distributedConsistentDatabase.requestHandler.pojo.JoinClusterRequest;
import com.distributedConsistentDatabase.requestHandler.pojo.KeyValuePutRequest;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.WebResource.Builder;

@SuppressWarnings("unchecked")
@RunWith(PowerMockRunner.class)
@PrepareForTest({WebResource.class, Builder.class})
public class DistributedConsistentDatabaseSAOTest {

    private DistributedConsistentDatabaseSAO dcdbSAO;
    private Client mockClient = EasyMock.createNiceMock(Client.class);
    private WebResource mockResource = EasyMock.createNiceMock(WebResource.class);
    private Builder mockBuilder = EasyMock.createNiceMock(Builder.class);

    @Before
    public void setUp() throws Exception {
        resetMocks();
        dcdbSAO = new DistributedConsistentDatabaseSAO(mockClient);
    }

    @After
    public void tearDown() throws Exception {
        verifyMocks();
    }

    @Test
    public void testAddClusterNode_SuccessResponse() {
        final ClusterNode target = getRandomClusterNode();
        final ClusterNode source = getRandomClusterNode();
        final Capture<JoinClusterRequest> requestCapture = new Capture<JoinClusterRequest>();
        staticWebResourceMock(target);
        EasyMock.expect(mockBuilder.post(EasyMock.anyObject(Class.class), EasyMock.capture(requestCapture))).andReturn(
            new ClientResponseStub(Status.OK.getStatusCode(), null)).anyTimes();

        replayMocks();

        Assert.assertTrue(dcdbSAO.addClusterNode(target, source));
        Assert.assertTrue(requestCapture.hasCaptured());
        Assert.assertEquals(requestCapture.getValue().getNode(), source);
    }

    @Test
    public void testAddClusterNode_FailureResponse() {
        final ClusterNode target = getRandomClusterNode();
        final ClusterNode source = getRandomClusterNode();
        final Capture<JoinClusterRequest> requestCapture = new Capture<JoinClusterRequest>();
        staticWebResourceMock(target);
        EasyMock.expect(mockBuilder.post(EasyMock.anyObject(Class.class), EasyMock.capture(requestCapture))).andReturn(
            new ClientResponseStub(Status.SERVICE_UNAVAILABLE.getStatusCode(), null));

        replayMocks();

        Assert.assertFalse(dcdbSAO.addClusterNode(target, source));
        Assert.assertTrue(requestCapture.hasCaptured());
        Assert.assertEquals(requestCapture.getValue().getNode(), source);
    }

    @Test
    public void testDeleteValue_SuccessResponse() {
        final ClusterNode node = getRandomClusterNode();
        final String key = UUID.randomUUID().toString();
        staticWebResourceMock(node);
        EasyMock.expect(mockBuilder.delete(ClientResponse.class)).andReturn(
            new ClientResponseStub(Status.OK.getStatusCode(), null));

        replayMocks();
        Assert.assertFalse(dcdbSAO.deleteValue(node, key));
    }

    @Test(expected = IllegalStateException.class)
    public void testDeleteValue_FailureResponse() {
        final ClusterNode node = getRandomClusterNode();
        final String key = UUID.randomUUID().toString();
        staticWebResourceMock(node);
        EasyMock.expect(mockBuilder.delete(ClientResponse.class)).andReturn(
            new ClientResponseStub(Status.SERVICE_UNAVAILABLE.getStatusCode(), null));

        replayMocks();
        dcdbSAO.deleteValue(node, key);
    }

    @Test
    public void testInternalDeleteValue_SuccessResponse() {
        final ClusterNode node = getRandomClusterNode();
        final String key = UUID.randomUUID().toString();
        staticWebResourceMock(node);
        EasyMock.expect(mockBuilder.delete(ClientResponse.class)).andReturn(
            new ClientResponseStub(Status.OK.getStatusCode(), null));

        replayMocks();
        Assert.assertFalse(dcdbSAO.internalDeleteValue(node, key));
    }

    @Test(expected = IllegalStateException.class)
    public void testInternalDeleteValue_FailureResponse() {
        final ClusterNode node = getRandomClusterNode();
        final String key = UUID.randomUUID().toString();
        staticWebResourceMock(node);
        EasyMock.expect(mockBuilder.delete(ClientResponse.class)).andReturn(
            new ClientResponseStub(Status.SERVICE_UNAVAILABLE.getStatusCode(), null));

        replayMocks();
        dcdbSAO.internalDeleteValue(node, key);
    }

    @Test
    public void testInternalGetValue_SuccessResponse() {
        final ClusterNode node = getRandomClusterNode();
        final String key = UUID.randomUUID().toString();
        final String expectedValue = UUID.randomUUID().toString();

        staticWebResourceMock(node);
        EasyMock.expect(mockBuilder.get(ClientResponse.class)).andReturn(
            new ClientResponseStub(Status.OK.getStatusCode(), expectedValue));

        replayMocks();
        Assert.assertEquals(dcdbSAO.internalGetValue(node, key), expectedValue);
    }

    @Test(expected = IllegalStateException.class)
    public void testInternalGetValue_FailureResponse() {
        final ClusterNode node = getRandomClusterNode();
        final String key = UUID.randomUUID().toString();

        staticWebResourceMock(node);
        EasyMock.expect(mockBuilder.get(ClientResponse.class)).andReturn(
            new ClientResponseStub(Status.SERVICE_UNAVAILABLE.getStatusCode(), null));

        replayMocks();
        dcdbSAO.internalGetValue(node, key);
    }

    @Test
    public void testInternalPutValue_SuccessResponse() {
        final ClusterNode node = getRandomClusterNode();
        final String key = UUID.randomUUID().toString();
        final String value = UUID.randomUUID().toString();
        final Capture<KeyValuePutRequest> requestCapture = new Capture<>();
        staticWebResourceMock(node);
        EasyMock.expect(mockBuilder.post(EasyMock.anyObject(Class.class), EasyMock.capture(requestCapture))).andReturn(
            new ClientResponseStub(Status.OK.getStatusCode(), null));

        replayMocks();
        Assert.assertFalse(dcdbSAO.internalPutValue(node, key, value));
        Assert.assertTrue(requestCapture.hasCaptured());
        Assert.assertEquals(requestCapture.getValue().getRequest().getKey(), key);
        Assert.assertEquals(requestCapture.getValue().getRequest().getValue(), value);
    }

    @Test(expected = IllegalStateException.class)
    public void testInternalPutValue_FailureResponse() {
        final ClusterNode node = getRandomClusterNode();
        final String key = UUID.randomUUID().toString();
        final String value = UUID.randomUUID().toString();
        staticWebResourceMock(node);
        EasyMock.expect(mockBuilder.post(EasyMock.anyObject(Class.class), EasyMock.anyObject(KeyValuePutRequest.class)))
            .andReturn(new ClientResponseStub(Status.SERVICE_UNAVAILABLE.getStatusCode(), null));

        replayMocks();
        dcdbSAO.internalPutValue(node, key, value);
    }

    @Test
    public void testPutValue_SuccessResponse() {
        final ClusterNode node = getRandomClusterNode();
        final String key = UUID.randomUUID().toString();
        final String value = UUID.randomUUID().toString();
        final Capture<KeyValuePutRequest> requestCapture = new Capture<>();
        staticWebResourceMock(node);
        EasyMock.expect(mockBuilder.post(EasyMock.anyObject(Class.class), EasyMock.capture(requestCapture))).andReturn(
            new ClientResponseStub(Status.OK.getStatusCode(), null));

        replayMocks();
        Assert.assertFalse(dcdbSAO.putValue(node, key, value));
        Assert.assertTrue(requestCapture.hasCaptured());
        Assert.assertEquals(requestCapture.getValue().getRequest().getKey(), key);
        Assert.assertEquals(requestCapture.getValue().getRequest().getValue(), value);
    }

    @Test(expected = IllegalStateException.class)
    public void testPutValue_FailureResponse() {
        final ClusterNode node = getRandomClusterNode();
        final String key = UUID.randomUUID().toString();
        final String value = UUID.randomUUID().toString();
        staticWebResourceMock(node);
        EasyMock.expect(mockBuilder.post(EasyMock.anyObject(Class.class), EasyMock.anyObject(KeyValuePutRequest.class)))
            .andReturn(new ClientResponseStub(Status.SERVICE_UNAVAILABLE.getStatusCode(), null));

        replayMocks();
        dcdbSAO.putValue(node, key, value);
    }

    @Test
    public void testHeartbeat_SuccessResponse() {
        final ClusterNode node = getRandomClusterNode();
        staticWebResourceMock(node);
        EasyMock.expect(mockBuilder.get(ClientResponse.class)).andReturn(
            new ClientResponseStub(Status.OK.getStatusCode(), null));

        replayMocks();

        Assert.assertTrue(dcdbSAO.isHeartbeatSuccessfull(node));
    }

    @Test
    public void testHeartbeat_FailureResponse() {
        final ClusterNode node = getRandomClusterNode();
        staticWebResourceMock(node);
        EasyMock.expect(mockBuilder.get(ClientResponse.class)).andReturn(
            new ClientResponseStub(Status.SERVICE_UNAVAILABLE.getStatusCode(), null));

        replayMocks();

        Assert.assertFalse(dcdbSAO.isHeartbeatSuccessfull(node));
    }

    private ClusterNode getRandomClusterNode() {
        final ClusterNode node = new ClusterNode();
        node.setIp(UUID.randomUUID().toString());
        node.setPort(UUID.randomUUID().toString());
        return node;
    }

    private void staticWebResourceMock(final ClusterNode target) {
        EasyMock.expect(mockClient.resource(DistributedConsistentDatabaseSAO.createBaseUrl(target)))
            .andReturn(mockResource);
        EasyMock.expect(mockResource.path(EasyMock.anyObject(String.class))).andReturn(mockResource).anyTimes();
        EasyMock.expect(mockResource.accept(EasyMock.anyObject(String.class))).andReturn(mockBuilder).anyTimes();
        EasyMock.expect(mockResource.type(EasyMock.anyObject(String.class))).andReturn(mockBuilder).anyTimes();
        EasyMock.expect(mockBuilder.type(EasyMock.anyObject(String.class))).andReturn(mockBuilder).anyTimes();
        EasyMock.expect(mockBuilder.accept(EasyMock.anyObject(String.class))).andReturn(mockBuilder).anyTimes();
    }

    private void resetMocks() {
        EasyMock.reset(mockClient, mockResource, mockBuilder);
    }

    private void replayMocks() {
        EasyMock.replay(mockClient, mockResource, mockBuilder);
    }

    private void verifyMocks() {
        EasyMock.verify(mockClient, mockResource, mockBuilder);
    }
}
