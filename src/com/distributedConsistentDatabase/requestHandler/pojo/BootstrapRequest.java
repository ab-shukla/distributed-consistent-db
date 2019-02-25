package com.distributedConsistentDatabase.requestHandler.pojo;

import com.distributedConsistentDatabase.cluster.pojo.ClusterNode;

public class BootstrapRequest {
    private ClusterNode seedServer;
    private String ip;
    private String port;
    private String nodeId;

    public ClusterNode getSeedServer() {
        return seedServer;
    }

    public void setSeedServer(ClusterNode seedServer) {
        this.seedServer = seedServer;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }
}
