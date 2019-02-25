package com.distributedConsistentDatabase.requestHandler.pojo;

import java.util.List;

import com.distributedConsistentDatabase.cluster.pojo.ClusterNode;

public class GetClusterResponse {
    private List<ClusterNode> serverList;

    public List<ClusterNode> getServerList() {
        return serverList;
    }

    public void setServerList(List<ClusterNode> serverList) {
        this.serverList = serverList;
    }
}
