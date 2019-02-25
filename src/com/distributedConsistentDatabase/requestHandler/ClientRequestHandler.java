package com.distributedConsistentDatabase.requestHandler;

import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.distributedConsistentDatabase.cluster.NodeManager;
import com.distributedConsistentDatabase.cluster.pojo.ClusterNode;
import com.distributedConsistentDatabase.requestHandler.pojo.BootstrapRequest;
import com.distributedConsistentDatabase.requestHandler.pojo.GetClusterResponse;
import com.distributedConsistentDatabase.requestHandler.pojo.JoinClusterRequest;
import com.distributedConsistentDatabase.requestHandler.pojo.KeyValuePutRequest;
import com.distributedConsistentDatabase.sao.DistributedConsistentDatabaseSAO;
import com.sun.jersey.spi.resource.Singleton;

@Path("/")
@Singleton
public class ClientRequestHandler {
    private NodeManager nodeManager;
    private DistributedConsistentDatabaseSAO dcdbSao;

    public ClientRequestHandler() {
        dcdbSao = new DistributedConsistentDatabaseSAO();
    }

    @GET
    @Path("/test")
    @Produces(MediaType.APPLICATION_JSON)
    public Response options() {
        return Response.status(Status.OK).entity("Abhishek").build();
    }

    @POST
    @Path("/keyValuePair")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response putValue(final KeyValuePutRequest putRequest) {
        try {
            final boolean result =
                nodeManager.putValueToCluster(putRequest.getRequest().getKey(), putRequest.getRequest().getValue());
            return Response.status(Status.OK).entity(result ? "TRUE" : "FALSE").build();
        } catch (final IllegalStateException e) {
            return Response.status(Status.SERVICE_UNAVAILABLE).entity(e).build();
        } catch (final Exception e) {
            return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e).build();
        }
    }

    @GET
    @Path("/keyValuePair/{param}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getValue(@PathParam("param") String key) {
        try {
            final String value = nodeManager.getValueFromCluster(key);
            return Response.status(Status.OK).entity(value).build();
        } catch (final Exception e) {
            return Response.status(Status.INTERNAL_SERVER_ERROR).entity(null).build();
        }
    }

    @DELETE
    @Path("keyValuePair/{key}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteValue(@PathParam("param") String key) {
        try {
            final boolean deleteResponse = nodeManager.deleteValueFromCluster(key);
            return Response.status(Status.OK).entity(deleteResponse ? "TRUE" : "FALSE").build();
        } catch (final Exception e) {
            return Response.status(Status.SERVICE_UNAVAILABLE).entity(e).build();
        }
    }

    @POST
    @Path("internal/keyValuePair")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response internalPutValue(final KeyValuePutRequest putRequest) {
        try {
            final boolean internalPutResponse =
                nodeManager.putValue(putRequest.getRequest().getKey(), putRequest.getRequest().getValue());
            return Response.status(Status.OK).entity(internalPutResponse ? "TRUE" : "FALSE").build();
        } catch (final Exception e) {
            return Response.status(Status.SERVICE_UNAVAILABLE).entity(e).build();
        }
    }

    @DELETE
    @Path("internal/keyValuePair/{key}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response internalDeleteValue(@PathParam("param") String key) {
        try {
            final boolean internalDeleteResponse = nodeManager.delete(key);
            return Response.status(Status.OK).entity(internalDeleteResponse ? "TRUE" : "FALSE").build();
        } catch (final Exception e) {
            return Response.status(Status.SERVICE_UNAVAILABLE).entity(e).build();
        }
    }

    @GET
    @Path("internal/keyValuePair/{param}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response internalGetValue(@PathParam("param") String key) {
        return Response.status(200).entity(nodeManager.getValue(key)).build();
    }

    @POST
    @Path("internal/bootstrap")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response bootstrap(final BootstrapRequest bootstrapRequest) {
        final ClusterNode currentNode = new ClusterNode();
        currentNode.setNodeId(Integer.valueOf(bootstrapRequest.getNodeId()));
        currentNode.setIp(bootstrapRequest.getIp());
        currentNode.setPort(bootstrapRequest.getPort());

        //initialize node manager
        nodeManager = NodeManager.of(currentNode);
        // Select the first seed server, if present, and get cluster details from it.
        if (bootstrapRequest.getSeedServer() != null) {
            final List<ClusterNode> clusterNodeList =
                dcdbSao.getClusterDetails(bootstrapRequest.getSeedServer());
            // Add the current node to all cluster nodes
            for (final ClusterNode node : clusterNodeList) {
                dcdbSao.addClusterNode(node, currentNode);
                nodeManager.addOtherClusterNode(node);
            }
        }

        return Response.status(200).entity("Bootstraped").build();
    }

    @GET
    @Path("internal/heartbeat")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getHeartbeat() {
        this.nodeManager.isPinged();
        final String output = "I am up";
        return Response.status(200).entity(output).build();
    }

    @POST
    @Path("internal/addClusterNode")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response addClusterNode(final JoinClusterRequest joinClusterRequest) {
        this.nodeManager.addOtherClusterNode(joinClusterRequest.getNode());
        return Response.status(200).build();
    }

    @GET
    @Path("internal/getCluster")
    @Produces(MediaType.APPLICATION_JSON)
    public GetClusterResponse getClusterDetails() {
        GetClusterResponse response = new GetClusterResponse();
        response.setServerList(this.nodeManager.getClusterDetails(null));
        return response;
    }

    @GET
    @Path("internal/getClusterLeader")
    @Produces(MediaType.APPLICATION_JSON)
    public Response isLeader() {
        return Response.status(Status.OK).entity(nodeManager.getClusterLeader(null)).build();
    }
}
