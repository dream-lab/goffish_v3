/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package in.dream_lab.goffish.hama;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.Partitioner;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.commons.util.KeyValuePair;
import org.apache.hama.util.ReflectionUtils;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.ISubgraph;
import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.hama.api.IControlMessage;
import in.dream_lab.goffish.hama.api.IReader;
import in.dream_lab.goffish.hama.utils.DisjointSets;
import in.dream_lab.goffish.api.IMessage;
import in.dream_lab.goffish.api.IRemoteVertex;


/**
 * 
 * @author humus
 *
 * @param <S>
 * @param <V>
 * @param <E>
 * @param <K>
 * @param <M>
 * 
 *          Same as LongTextAdjacency list reader but more scalable in cases
 *          where edges grow faster than vertices.
 * 
 *          Perform better on highly connected graphs as it sends vertices
 *          instead of remote vertices to find subgraphID
 * 
 *          Reads graph in the adjacency list format: VID Sink1 Sink2 ...
 */

public class DenseGraphLongTextAdjacencyListReader<S extends Writable, V extends Writable, E extends Writable, K extends Writable, M extends Writable>
    implements
    IReader<Writable, Writable, Writable, Writable, S, V, E, LongWritable, LongWritable, LongWritable> {

  public static final Log LOG = LogFactory
      .getLog(DenseGraphLongTextAdjacencyListReader.class);

  Map<Long, IVertex<V, E, LongWritable, LongWritable>> vertexMap;
  Map<Long, IRemoteVertex<V, E, LongWritable, LongWritable, LongWritable>> remoteVertexMap;
  BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer;
  private Map<K, Integer> subgraphPartitionMap;
  // TODO : Change to Long from LongWritable
  private Map<LongWritable, LongWritable> vertexSubgraphMap;

  public DenseGraphLongTextAdjacencyListReader(
      BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer,
      Map<K, Integer> subgraphPartitionMap) {
    this.peer = peer;
    this.subgraphPartitionMap = subgraphPartitionMap;
    this.vertexSubgraphMap = new HashMap<LongWritable, LongWritable>();
  }

  /*
   * Returns the list of subgraphs belonging to the current partition
   */
  @Override
  public List<ISubgraph<S, V, E, LongWritable, LongWritable, LongWritable>> getSubgraphs()
      throws IOException, SyncException, InterruptedException {

    /* Used for logging */
    Runtime runtime = Runtime.getRuntime();
    int mb = 1024*1024;
    

    LOG.info("Free Memory in Reader: " + runtime.freeMemory() / mb + " Total Memory: " + runtime.totalMemory() / mb);
    
    LOG.info("Free Memory after Reaching reader " + Runtime.getRuntime().freeMemory());
    
    KeyValuePair<Writable, Writable> pair;
    long edgeCount = 0;

    vertexMap = Maps.newHashMap();
    remoteVertexMap = Maps.newHashMap();

    LOG.info("SETUP Starting Free Memory: " + runtime.freeMemory() / mb + " Total Memory: " + runtime.totalMemory() / mb);
    LOG.info("SETUP Starting " + peer.getPeerIndex() + " Memory: "
        + Runtime.getRuntime().freeMemory());
    int count = -1;

    while ((pair = peer.readNext()) != null) {
      count++;
      // NOTE: Confirm that data starts from value and not from key.
      String stringInput = pair.getValue().toString();
      String vertexValue[] = stringInput.split("\\s+");
      
      LongWritable vertexID = new LongWritable(Long.parseLong(vertexValue[0]));
      List<IEdge<E, LongWritable, LongWritable>> _adjList = new ArrayList<IEdge<E, LongWritable, LongWritable>>();

      for (int j = 1; j < vertexValue.length; j++) {
        LongWritable sinkID = new LongWritable(Long.parseLong(vertexValue[j]));
        LongWritable edgeID = new LongWritable(
                edgeCount++ | (((long) peer.getPeerIndex()) << 32));
        Edge<E, LongWritable, LongWritable> e = new Edge<E, LongWritable, LongWritable>(edgeID, sinkID);
        _adjList.add(e);
      }
      vertexMap.put(vertexID.get(), createVertexInstance(vertexID, _adjList));

    }

    LOG.info("Number of Vertices: " + vertexMap.size() + " Edges: " + edgeCount);

    LOG.info("Free Memory: " + runtime.freeMemory() / mb + " Total Memory: " + runtime.totalMemory() / mb);
    System.gc();
    peer.sync();
    LOG.info("Creating Remote Vertex Objects");

    /* Create remote vertex objects. */
    for (IVertex<V, E, LongWritable, LongWritable> vertex : vertexMap.values()) {
      for (IEdge<E, LongWritable, LongWritable> e : vertex.getOutEdges()) {
        LongWritable sinkID = e.getSinkVertexId();
        if (!vertexMap.containsKey(sinkID.get())) {
          IRemoteVertex<V, E, LongWritable, LongWritable, LongWritable> sink = new RemoteVertex<>(
              sinkID);
          remoteVertexMap.put(sinkID.get(), sink);
        }
      }
    }

    peer.sync();

    Partition<S, V, E, LongWritable, LongWritable, LongWritable> partition = new Partition<>(
        peer.getPeerIndex());

    LOG.info("Calling formSubgraph()");

    formSubgraphs(partition);

    //clearing used memory
    vertexMap = null;

    LOG.info("Done with formSubgraph()");
    /*
     * Tell other partitions our Vertex Ids and their subgraphIDs
     */
    Message<LongWritable, LongWritable> question = new Message<LongWritable, LongWritable>();
    ControlMessage controlInfo = new ControlMessage();
    controlInfo.setTransmissionType(IControlMessage.TransmissionType.BROADCAST);
    question.setControlInfo(controlInfo);
    /*
     * Message format being sent: subgraphID1 count1 vertex1 vertex2 ... subgraphID2 count2 vertex1 vertex2 ...
     */
    for (ISubgraph<S, V, E, LongWritable, LongWritable, LongWritable> subgraphs : partition.getSubgraphs()) {
      controlInfo.addextraInfo(Longs.toByteArray(subgraphs.getSubgraphId().get()));
      controlInfo.addextraInfo(Longs.toByteArray(subgraphs.getLocalVertexCount()));
      for (IVertex<V, E, LongWritable, LongWritable> v : subgraphs.getLocalVertices()) {
        byte vertexIDbytes[] = Longs.toByteArray(v.getVertexId().get());
        controlInfo.addextraInfo(vertexIDbytes);
      }
    }
    sendToAllPartitions(question);

    LOG.info("Completed first superstep in reader");
    System.out.println("Before 2nd Superstep "
        + (runtime.totalMemory() - runtime.freeMemory()) / mb);
    peer.sync();
    LOG.info("Started superstep 2 in reader");

    Message<LongWritable, LongWritable> msg;
    Map<Integer, List<Message<LongWritable, LongWritable>>> replyMessages = new HashMap<Integer, List<Message<LongWritable, LongWritable>>>();
    // Receiving 1 message per partition
    while ((msg = (Message<LongWritable, LongWritable>) peer
        .getCurrentMessage()) != null) {
      /*
       * Subgraph Partition mapping broadcast Format of received message:
       * partitionID subgraphID1 subgraphID2 ...
       */
      if (msg.getMessageType() == Message.MessageType.SUBGRAPH) {
        Iterable<BytesWritable> subgraphList = ((ControlMessage) msg
            .getControlInfo()).getExtraInfo();

        Integer partitionID = Ints
            .fromByteArray(subgraphList.iterator().next().getBytes());

        for (BytesWritable subgraphListElement : Iterables.skip(subgraphList,
            1)) {
          LongWritable subgraphID = new LongWritable(
              Longs.fromByteArray(subgraphListElement.getBytes()));
          subgraphPartitionMap.put((K) subgraphID, partitionID);
        }
        continue;
      }

      /*
       * receiving vertices to set Remote Vertex subgraph id.
       */
      Iterator<BytesWritable> remoteVertexQuery = ((ControlMessage) msg
          .getControlInfo()).getExtraInfo().iterator();

      while (remoteVertexQuery.hasNext()) {
        Long subgraphID = Longs.fromByteArray(remoteVertexQuery.next().getBytes());
        Long vertexCount = Longs.fromByteArray(remoteVertexQuery.next().getBytes());
        for (long i = 0; i < vertexCount; i++) {
          Long remoteVertexID = Longs.fromByteArray(remoteVertexQuery.next().getBytes());
          if (remoteVertexMap.containsKey(remoteVertexID)) {
            RemoteVertex<V, E, LongWritable, LongWritable, LongWritable> sink = (RemoteVertex<V, E, LongWritable, LongWritable, LongWritable>) remoteVertexMap
                .get(remoteVertexID);
            sink.setSubgraphID(new LongWritable(subgraphID));
          }
        }
      }
    }

    LOG.info("Completed 2nd superstep in reader");
    LOG.info("Reader finished");
    return partition.getSubgraphs();
  }

  private IVertex<V, E, LongWritable, LongWritable> createVertexInstance(LongWritable vertexID, List<IEdge<E, LongWritable, LongWritable>> adjList) {
    return ReflectionUtils.newInstance(GraphJobRunner.VERTEX_CLASS, new Class<?>[] {Writable.class, Iterable.class},
            new Object[] {vertexID, adjList});
  }

  private void sendToAllPartitions(Message<LongWritable, LongWritable> message)
      throws IOException {
    for (String peerName : peer.getAllPeerNames()) {
      peer.send(peerName, (Message<K, M>) message);
    }
  }

  /* Forms subgraphs by finding (weakly) connected components. */
  void formSubgraphs(
      Partition<S, V, E, LongWritable, LongWritable, LongWritable> partition)
      throws IOException, SyncException, InterruptedException {

    long subgraphCount = 0;
    Message<LongWritable, LongWritable> subgraphLocationBroadcast = new Message<LongWritable, LongWritable>();

    subgraphLocationBroadcast.setMessageType(Message.MessageType.SUBGRAPH);
    ControlMessage controlInfo = new ControlMessage();
    controlInfo.setTransmissionType(IControlMessage.TransmissionType.BROADCAST);
    subgraphLocationBroadcast.setControlInfo(controlInfo);

    byte partitionBytes[] = Ints.toByteArray(peer.getPeerIndex());
    controlInfo.addextraInfo(partitionBytes);

    
    // initialize disjoint set
    DisjointSets<IVertex<V, E, LongWritable, LongWritable>> ds = new DisjointSets<IVertex<V, E, LongWritable, LongWritable>>(
        vertexMap.size() + remoteVertexMap.size());
    for (IVertex<V, E, LongWritable, LongWritable> vertex : vertexMap.values()) {
      ds.addSet(vertex);
    }
    for (IVertex<V, E, LongWritable, LongWritable> vertex : remoteVertexMap.values()) {
      ds.addSet(vertex);
    }

    // union edge pairs
    for (IVertex<V, E, LongWritable, LongWritable> vertex : vertexMap.values()) {
      for (IEdge<E, LongWritable, LongWritable> edge : vertex.getOutEdges()) {
        IVertex<V, E, LongWritable, LongWritable> sink = vertexMap
            .get(edge.getSinkVertexId().get());
        if (sink == null) {
          sink = remoteVertexMap.get(edge.getSinkVertexId().get());
        }
        ds.union(vertex, sink);
      }
    }

    Collection<? extends Collection<IVertex<V, E, LongWritable, LongWritable>>> components = ds
        .retrieveSets();

    for (Collection<IVertex<V, E, LongWritable, LongWritable>> component : components) {
      LongWritable subgraphID = new LongWritable(
          subgraphCount++ | (((long) partition.getPartitionId()) << 32));
      Subgraph<S, V, E, LongWritable, LongWritable, LongWritable> subgraph = new Subgraph<S, V, E, LongWritable, LongWritable, LongWritable>(
          peer.getPeerIndex(), subgraphID);

      for (IVertex<V, E, LongWritable, LongWritable> vertex : component) {
        subgraph.addVertex(vertex);

        // Dont add remote vertices to the VertexSubgraphMap as remote vertex
        // subgraphID is unknown
        if (!vertex.isRemote()) {
          vertexSubgraphMap.put(vertex.getVertexId(), subgraph.getSubgraphId());
        }
      }

      partition.addSubgraph(subgraph);

      byte subgraphIDbytes[] = Longs.toByteArray(subgraphID.get());
      controlInfo.addextraInfo(subgraphIDbytes);

    }
    sendToAllPartitions(subgraphLocationBroadcast);
  }
}
