package com.distributedConsistentDatabase.sao;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import com.distributedConsistentDatabase.cluster.pojo.ClusterNode;
import com.distributedConsistentDatabase.requestHandler.pojo.GetClusterResponse;
import com.distributedConsistentDatabase.requestHandler.pojo.JoinClusterRequest;
import com.distributedConsistentDatabase.requestHandler.pojo.KeyValueDetails;
import com.distributedConsistentDatabase.requestHandler.pojo.KeyValuePutRequest;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;

public class DistributedConsistentDatabaseSAO {

    public boolean putValue(final ClusterNode node, final String key, final String value) {
        final String baseUrl = createBaseUrl(node);
        final Client client = getClient();
        final WebResource webTarget = client.resource(baseUrl).path("keyValuePair");
        final KeyValueDetails  kvDetails = new KeyValueDetails();
        kvDetails.setKey(key);
        kvDetails.setValue(value);
        final KeyValuePutRequest putRequest = new KeyValuePutRequest();
        putRequest.setRequest(kvDetails);
        final ClientResponse invocationResponse = webTarget.type(MediaType.APPLICATION_JSON)
            .accept(MediaType.APPLICATION_JSON)
            .post(ClientResponse.class, putRequest);

        if (invocationResponse.getStatus() == Status.OK.getStatusCode()) {
            return "TRUE".equals(invocationResponse.getEntity(String.class));
        }
        throw new IllegalStateException();
    }

    public boolean internalPutValue(final ClusterNode node, final String key, final String value) {
        final String baseUrl = createBaseUrl(node);
        final Client client = getClient();
        final WebResource webTarget = client.resource(baseUrl).path("internal").path("keyValuePair");
        final KeyValueDetails kvDetails = new KeyValueDetails();
        kvDetails.setKey(key);
        kvDetails.setValue(value);

        final KeyValuePutRequest putRequest = new KeyValuePutRequest();
        putRequest.setRequest(kvDetails);
        final ClientResponse invocationResponse = webTarget.type(MediaType.APPLICATION_JSON)
            .accept(MediaType.APPLICATION_JSON).post(ClientResponse.class, putRequest);

        if (invocationResponse.getStatus() == Status.OK.getStatusCode()) {
            return "TRUE".equals(invocationResponse.getEntity(String.class));
        }
        throw new IllegalStateException();
    }

    public String internalGetValue(final ClusterNode node, final String key) {
        final String baseUrl = createBaseUrl(node);
        final Client client = getClient();
        final WebResource webTarget = client.resource(baseUrl).path("internal").path("keyValuePair").path(key);
        final ClientResponse invocationResponse = webTarget.accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
        if (invocationResponse.getStatus() == Status.OK.getStatusCode()) {
            return invocationResponse.getEntity(String.class);
        }
        throw new IllegalStateException();
    }

    public boolean isHeartbeatSuccessfull(final ClusterNode node) {
        final String baseUrl = createBaseUrl(node);
        final Client client = getClient();
        final WebResource webTarget = client.resource(baseUrl).path("internal").path("heartbeat");
        final ClientResponse invocationResponse = webTarget.accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
        if (invocationResponse.getStatus() == Status.OK.getStatusCode()) {
            return true;
        }
        return false;
    }

    public boolean addClusterNode(final ClusterNode targetNode, final ClusterNode currentNode) {
        final String baseUrl = createBaseUrl(targetNode);
        final Client client = getClient();
        final WebResource webTarget = client.resource(baseUrl).path("internal").path("addClusterNode");
        final JoinClusterRequest joinClusterRequest = new JoinClusterRequest();
        joinClusterRequest.setNode(currentNode);
        final ClientResponse invocationResponse = webTarget.type(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON)
            .post(ClientResponse.class, joinClusterRequest);

        if (invocationResponse.getStatus() == Status.OK.getStatusCode()) {
            return true;
        }
        return false;
    }

    public List<ClusterNode> getClusterDetails(final ClusterNode node) {
        final String baseUrl = createBaseUrl(node);
        final Client client = getClient();
        final WebResource webTarget = client.resource(baseUrl).path("internal").path("getCluster");
        final ClientResponse invocationResponse = webTarget.accept(MediaType.APPLICATION_JSON)
            .get(ClientResponse.class);

        if (invocationResponse.getStatus() == Status.OK.getStatusCode()) {
            return invocationResponse.getEntity(GetClusterResponse.class).getServerList();
        }
        return new ArrayList<>();
    }

    private String createBaseUrl(final ClusterNode node) {
        return new StringBuilder()
            .append("http://")
            .append(node.getIp())
            .append(":")
            .append(node.getPort())
            .append("/DistributedConsistentDatabase").toString();
    }

    private Client getClient() {
        final ClientConfig clientConfig = new DefaultClientConfig();
        clientConfig.getFeatures().put(
                JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
        return Client.create(clientConfig);
    }
}
