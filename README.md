# Distributed consistent database
Sample implementation of a distributed consistent data base. The implementation enables creation of a cluster of servers which store key-value data in a consistent manner.

# Notice
This is only an initial prototype.

# Overall design
## Architecture
The core implementation uses the concept of a cluster with odd number of nodes. The cluster has 1 leader and others act as followers. Each node in the cluster is identified by a positive *nodeId* which has to be unique in the cluster. All the nodes of the cluster try to store all the key-value data being submitted to the cluster. The fact that a read or write was successful is determined by the concept of Quorum where Quorum means (Cluster size / 2) + 1. If the Quorum cannot be achieved, the operation is considered a failure.

## Leader node
The leader node has the responsibility of pinging all the followers and keeping track of available. Leader election is based on the minimum value of *nodeId*. All the nodes of the cluster assume the leader to have the minimum *nodeId*. If the leader cannot reach a follower for 4 seconds, it assumes the follower has died or partitioned and removes it from the cluster. Since the followers only know the leader node, they do not take any action on such events.
The leader node is responsible for performing any write operation (PUT, UPDATE, DELETE). If any of the follower nodes get a write request, they re-direct the write to the Leader.

## Follower node
The follower node has the responsibility of waiting for a ping from leader node. If a follower is not pinged for 10 seconds, it assumes that the leader has died, and assumes that the next minimum *nodeId* to be the next leader. It then waits to be pinged by the new leader node. Followers redirect any write operation to the Leader node. For read operations, the followers use the Quorum method to return the results from themselves.

## Key value store
Current implementation uses a in-memory key value store. It does not perform any disk-writes/ DB-log writes.

## Application layer
The entire implementation is exposed through REST APIs which are implemented using Jersey. The APIs are of two types
## Internal APIs
These are the APIs to be used by the cluster in itself. Not to be used by external clients. All these resources have *internal* in their resource path.
## External APIs
* Get (/keyValuePair/{key}): Gets the value for the key.
* Put (/keyValuePair): Puts the key/ value pair in the cluster. Returns "TRUE" if successful, "FALSE" otherwise.
* Bootstrap (internal/bootstrap): Bootstraps a node in the cluster making it available for use. User needs to provide a *seedServer* for configuration. Returns 200 OK when successful.

## Assumptions/ Limitations
* The current solution assumes the minimum cluster size of 5. And minimum quorum size of 3. If you add more nodes to the cluster, say 7, then the quorum size will increase accordingly
* All the data is attempted to be stored in all the nodes. The solution does not support data partioning out of the box. However, it provides an extension as ClusterMesh which is a collection of multiple data partitioned clusters.
* Serialized writes: Current implementation only performs 1 write at a time, with the extension of improving it to perform only *one write per key*.
* Concurrent addition of nodes in the cluster is not supported.
* Solution assumes unique positive *nodeId* for each node of the cluster.
* Solution does not provide data-durability. If a node goes down, the new node added will not have the data present in other nodes.
* User needs to bootstrap each node with an internal bootstrap API. We assumes that the seedServer in the bootstrap request is up and not network partitioned.

# Building the project
* Download the code
* mvn clean install
* Copy the generated war file. Setup the war in any tomcat implementation.

# Launching the cluster
* Start the first node. Call bootstrap API. Sample API:
```
curl -X POST -H 'Content-Type: application/json' -i http://localhost:8080/DistributedConsistentDatabase/internal/bootstrap --data '{
   "seedServer":null,
   "ip":"127.0.0.1",
   "port":"8080",
   "nodeId":"1"
}'
```

* Start other nodes. Call bootstrap API with the first (or any available) node as seedServce:
```
curl -X POST -H 'Content-Type: application/json' -i http://localhost:8081/DistributedConsistentDatabase/internal/bootstrap --data '{
   "seedServer":{
      "nodeId":"1",
      "ip":"127.0.0.1",
      "port":"8080"
   },
   "ip":"127.0.0.1",
   "port":"8081",
   "nodeId":"2"
}'
```

* Put data in the cluster. Sample API
```
curl -X POST -H 'Content-Type: application/json' -i http://localhost:8080/DistributedConsistentDatabase/keyValuePair --data '{
    "request": {
        "key":"1",
        "value":"test"
    }
}'
```
* Get data from the cluster. Sample API
```
curl -X GET -i http://localhost:8083/DistributedConsistentDatabase/keyValuePair/1
```

# Contact
For any queries, please contact abhishek_shukla99@yahoo.com
