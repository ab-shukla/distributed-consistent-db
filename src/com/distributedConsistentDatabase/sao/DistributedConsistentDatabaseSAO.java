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

/**
 * Service access object for DistributedConsistentDatabase web application. The class provides utility methods
 * to invoke the REST APIs exposed by the web app. Current implementation uses jersey client.
 * @author abshukla
 */
public class DistributedConsistentDatabaseSAO {
    private static final String KEY_VALUE_PAIR_PATH = "keyValuePair";
    private static final String INTERNAL_PATH = "internal";
    private static final String GET_CLUSTER_PATH = "getCluster";
    private static final String ADD_CLUSTER_NODE_PATH = "addClusterNode";
    private static final String HEARTBEAT_PATH = "heartbeat";
    private static final String RESPONSE_VALUE_TRUE = "TRUE";

    private final Client restClient;

    /**
     * Constructor
     * @param client : Jersey client object.
     */
    public DistributedConsistentDatabaseSAO(final Client client) {
        this.restClient = client;
    }

    /**
     * Makes the putValue call to the provided node with the given key and value details.
     * This is not an internal API call and will take affect as if called from an external client
     * and updates the complete cluster. Callers need ensure that this call is being made to the Leader node.
     * @param node : Cluster node to which putValue call is to be made.
     * @param key : key to put
     * @param value : value to put
     * @return : true if put is successful, false otherwise.
     */
    public boolean putValue(final ClusterNode node, final String key, final String value) {
        final String baseUrl = createBaseUrl(node);
        final WebResource webTarget = this.restClient.resource(baseUrl).path(KEY_VALUE_PAIR_PATH);
        final KeyValueDetails  kvDetails = new KeyValueDetails();
        kvDetails.setKey(key);
        kvDetails.setValue(value);
        final KeyValuePutRequest putRequest = new KeyValuePutRequest();
        putRequest.setRequest(kvDetails);
        final ClientResponse invocationResponse = webTarget.type(MediaType.APPLICATION_JSON)
            .accept(MediaType.APPLICATION_JSON)
            .post(ClientResponse.class, putRequest);

        if (invocationResponse.getStatus() == Status.OK.getStatusCode()) {
            return RESPONSE_VALUE_TRUE.equals(invocationResponse.getEntity(String.class));
        }
        throw new IllegalStateException();
    }

    /**
     * Makes the internalPutValue call to the provided node with the given key and value details.
     * This is an internal API call and will take affect only on the node it is called on.
     * @param node : Cluster node to which putValue call is to be made.
     * @param key : key to put
     * @param value : value to put
     * @return : true if put is successful, false otherwise.
     */
    public boolean internalPutValue(final ClusterNode node, final String key, final String value) {
        final String baseUrl = createBaseUrl(node);
        final WebResource webTarget = this.restClient.resource(baseUrl).path(INTERNAL_PATH).path(KEY_VALUE_PAIR_PATH);
        final KeyValueDetails kvDetails = new KeyValueDetails();
        kvDetails.setKey(key);
        kvDetails.setValue(value);

        final KeyValuePutRequest putRequest = new KeyValuePutRequest();
        putRequest.setRequest(kvDetails);
        final ClientResponse invocationResponse = webTarget.type(MediaType.APPLICATION_JSON)
            .accept(MediaType.APPLICATION_JSON).post(ClientResponse.class, putRequest);

        if (invocationResponse.getStatus() == Status.OK.getStatusCode()) {
            return RESPONSE_VALUE_TRUE.equals(invocationResponse.getEntity(String.class));
        }
        throw new IllegalStateException();
    }

    /**
     * Method to delete a key from the cluster. The method takes a node as input (Leader node).
     * @param node : node to make the call on.
     * @param key : key to delete from the cluster
     * @return : true if deleted, false if the key does not exist in the cluster.
     */
    public boolean deleteValue(final ClusterNode node, final String key) {
        final String baseUrl = createBaseUrl(node);
        final WebResource webTarget = this.restClient.resource(baseUrl).path(KEY_VALUE_PAIR_PATH).path(key);

        final ClientResponse invocationResponse = webTarget.accept(MediaType.APPLICATION_JSON)
            .delete(ClientResponse.class);

        if (invocationResponse.getStatus() == Status.OK.getStatusCode()) {
            return RESPONSE_VALUE_TRUE.equals(invocationResponse.getEntity(String.class));
        }
        throw new IllegalStateException();
    }

    /**
     * Method to delete a key from the provided node.
     * @param node : node to make the call on.
     * @param key : key to delete from the node
     * @return : true if deleted, false if the key does not exist in the node.
     */
    public boolean internalDeleteValue(final ClusterNode node, final String key) {
        final String baseUrl = createBaseUrl(node);
        final WebResource webTarget = this.restClient.resource(baseUrl).path(INTERNAL_PATH).path(KEY_VALUE_PAIR_PATH).path(key);

        final ClientResponse invocationResponse = webTarget.accept(MediaType.APPLICATION_JSON)
            .delete(ClientResponse.class);

        if (invocationResponse.getStatus() == Status.OK.getStatusCode()) {
            return RESPONSE_VALUE_TRUE.equals(invocationResponse.getEntity(String.class));
        }
        throw new IllegalStateException();
    }

    /**
     * Method to get the value associated to a specific key in the node passed in the parameters.
     * @param node : node where the key is to be looked up.
     * @param key : Key to look up
     * @return : String value if found, null otherwise.
     */
    public String internalGetValue(final ClusterNode node, final String key) {
        final String baseUrl = createBaseUrl(node);
        final WebResource webTarget = this.restClient.resource(baseUrl).path(INTERNAL_PATH).path(KEY_VALUE_PAIR_PATH).path(key);
        final ClientResponse invocationResponse = webTarget.accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
        if (invocationResponse.getStatus() == Status.OK.getStatusCode()) {
            return invocationResponse.getEntity(String.class);
        }
        throw new IllegalStateException();
    }

    /**
     * Method to execute a heartbeat on the provided cluster node.
     * @param node : node to ping.
     * @return : true if successful, false otherwise.
     */
    public boolean isHeartbeatSuccessfull(final ClusterNode node) {
        final String baseUrl = createBaseUrl(node);
        final WebResource webTarget = this.restClient.resource(baseUrl).path(INTERNAL_PATH).path(HEARTBEAT_PATH);
        final ClientResponse invocationResponse = webTarget.accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
        if (invocationResponse.getStatus() == Status.OK.getStatusCode()) {
            return true;
        }
        return false;
    }

    /**
     * Method to add a new node to the existing cluster. This method is to be executed on all the existing nodes
     * of the cluster. This call makes sure that the target node is aware of the new node.
     * @param targetNode : Target node on which the current node will be added.
     * @param currentNode : Node to add to the cluster (new node)
     * @return : true if added successfully, false otherwise.
     */
    public boolean addClusterNode(final ClusterNode targetNode, final ClusterNode currentNode) {
        final String baseUrl = createBaseUrl(targetNode);
        final WebResource webTarget = this.restClient.resource(baseUrl).path(INTERNAL_PATH).path(ADD_CLUSTER_NODE_PATH);
        final JoinClusterRequest joinClusterRequest = new JoinClusterRequest();
        joinClusterRequest.setNode(currentNode);
        final ClientResponse invocationResponse = webTarget.type(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON)
            .post(ClientResponse.class, joinClusterRequest);

        if (invocationResponse.getStatus() == Status.OK.getStatusCode()) {
            return true;
        }
        return false;
    }

    /**
     * Method to get the cluster details (node list) from a specific node in the cluster.
     * @param node : node from where the cluster details are to be fetched.
     * @return : list of clusterNodes, empty list, if the node doesn't respond.
     */
    public List<ClusterNode> getClusterDetails(final ClusterNode node) {
        final String baseUrl = createBaseUrl(node);
        final WebResource webTarget = this.restClient.resource(baseUrl).path(INTERNAL_PATH).path(GET_CLUSTER_PATH);
        final ClientResponse invocationResponse = webTarget.accept(MediaType.APPLICATION_JSON)
            .get(ClientResponse.class);

        if (invocationResponse.getStatus() == Status.OK.getStatusCode()) {
            return invocationResponse.getEntity(GetClusterResponse.class).getServerList();
        }
        return new ArrayList<>();
    }

    protected static String createBaseUrl(final ClusterNode node) {
        return new StringBuilder()
            .append("http://")
            .append(node.getIp())
            .append(":")
            .append(node.getPort())
            .append("/DistributedConsistentDatabase").toString();
    }
}
