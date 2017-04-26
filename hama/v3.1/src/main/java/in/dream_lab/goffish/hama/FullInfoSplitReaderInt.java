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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import in.dream_lab.goffish.api.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.commons.util.KeyValuePair;

import com.google.common.primitives.Longs;

import in.dream_lab.goffish.hama.api.IControlMessage;
import in.dream_lab.goffish.hama.api.IReader;
import org.apache.hama.util.ReflectionUtils;

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
 * Expected format :
 * pid sgid     vid sinkid1 sgid1 pid1 sinkid2 sgid2 pid2 ...
 * 
 * VertexId - LongWritable
 * EdgeId - IntWritable
 * SubgraphId - LongWritable
 * 
 * As this reader takes split files it might also get those vertices that belong to other partitions
 * superstep 1 - shuffles the vertices around and send them to their respective partition(creates a few obj also)
 * superstep 2 - create objects(vertex,edge and remote vertex). send the subgraphIDs that we have to all other partitions.
 * Superstep 3 - generate subgraphPartitionMapping from the incoming msgs. 
 */
public class FullInfoSplitReaderInt<S extends Writable, V extends Writable, E extends Writable, K extends Writable, M extends Writable>
    implements
    IReader<Writable, Writable, Writable, Writable, S, V, E, LongWritable, IntWritable, LongWritable> {

  public static final Log LOG = LogFactory.getLog(FullInfoSplitReaderInt.class);

  private HamaConfiguration conf;
  private BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer;
  private Partition<S, V, E, LongWritable, IntWritable, LongWritable> partition;
  private Map<K, Integer> subgraphPartitionMap;
  private int edgeCount = 0;

  public FullInfoSplitReaderInt(
      BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer,
      Map<K, Integer> subgraphPartitionMap) {
    this.peer = peer;
    this.subgraphPartitionMap = subgraphPartitionMap;
    this.conf = peer.getConfiguration();
    partition = new Partition<>(peer.getPeerIndex());
  }

  @Override
  public List<ISubgraph<S, V, E, LongWritable, IntWritable, LongWritable>> getSubgraphs()
      throws IOException, SyncException, InterruptedException {

    KeyValuePair<Writable, Writable> pair;
    while ((pair = peer.readNext()) != null) {
      String stringInput = pair.getValue().toString();
      // pid is the first column and its range is 0 to max pid
      int partitionID = Integer
          .parseInt(stringInput.substring(0, stringInput.indexOf('\t')));
      LOG.debug("partitionID = "+partitionID);
      
      if (partitionID != peer.getPeerIndex()) {
        // send vertex to its correct partition
        Message<K, M> msg = new Message<>();
        msg.setMessageType(Message.MessageType.VERTEX);
        ControlMessage ctrl = new ControlMessage();
        ctrl.setTransmissionType(IControlMessage.TransmissionType.VERTEX);
        ctrl.addextraInfo(stringInput.getBytes());
        msg.setControlInfo(ctrl);
        peer.send(peer.getPeerName(partitionID), msg);

      } else {

        // belongs to this partition
        createVertex(stringInput);
      }
    }

    peer.sync();

    Message<K, M> msg;
    //recieve all incoming vertices
    while ((msg = peer.getCurrentMessage()) != null) {
      ControlMessage receivedCtrl = (ControlMessage) msg.getControlInfo();
      createVertex(new String(
          receivedCtrl.getExtraInfo().iterator().next().copyBytes()));
    }

    // broadcast all subgraphs belonging to this partition
    Message<K, M> subgraphMapppingMessage = new Message<>();
    subgraphMapppingMessage.setMessageType(Message.MessageType.CUSTOM_MESSAGE);
    ControlMessage controlInfo = new ControlMessage();
    controlInfo.setTransmissionType(IControlMessage.TransmissionType.BROADCAST);
    controlInfo.setPartitionID(peer.getPeerIndex());
    subgraphMapppingMessage.setControlInfo(controlInfo);
    for (ISubgraph<S, V, E, LongWritable, IntWritable, LongWritable> subgraph : partition
        .getSubgraphs()) {

      byte subgraphIDbytes[] = Longs
          .toByteArray(subgraph.getSubgraphId().get());
      controlInfo.addextraInfo(subgraphIDbytes);
    }

    sendToAllPartitions(subgraphMapppingMessage);

    peer.sync();
    Message<K, M> subgraphMappingInfoMessage;
    while ((subgraphMappingInfoMessage = peer.getCurrentMessage()) != null) {
      ControlMessage receivedCtrl = (ControlMessage) subgraphMappingInfoMessage.getControlInfo();
      Integer partitionID = receivedCtrl.getPartitionID();
      for (BytesWritable rawSubgraphID : receivedCtrl.getExtraInfo()) {
        LongWritable subgraphID = new LongWritable(
            Longs.fromByteArray(rawSubgraphID.copyBytes()));
        subgraphPartitionMap.put((K) subgraphID, partitionID);
      }
    }

    return partition.getSubgraphs();
  }

  private void sendToAllPartitions(Message<K, M> message) throws IOException {
    for (String peerName : peer.getAllPeerNames()) {
      peer.send(peerName, message);
    }
  }

  private void createVertex(String stringInput) {

    // belongs to this partition
    String vertexValue[] = stringInput.split("\\s+");

    LongWritable vertexID = new LongWritable(
        Long.parseLong(vertexValue[1]));
    int partitionID = Integer.parseInt(vertexValue[0]) - 1;
    LongWritable vertexSubgraphID = new LongWritable(
        Long.parseLong(vertexValue[2]));

    Subgraph<S, V, E, LongWritable, IntWritable, LongWritable> subgraph = (Subgraph<S, V, E, LongWritable, IntWritable, LongWritable>) partition
        .getSubgraph(vertexSubgraphID);

    if (subgraph == null) {
      subgraph = new Subgraph<S, V, E, LongWritable, IntWritable, LongWritable>(
          partitionID, vertexSubgraphID);
      partition.addSubgraph(subgraph);
    }
    List<IEdge<E, LongWritable, IntWritable>> _adjList = new ArrayList<IEdge<E, LongWritable, IntWritable>>();

    for (int j = 3; j < vertexValue.length; j++) {
      if (j + 3 > vertexValue.length) {
        LOG.debug("Incorrect length of line for vertex " + vertexID);
      }
      LongWritable sinkID = new LongWritable(Long.parseLong(vertexValue[j]));
      LongWritable sinkSubgraphID = new LongWritable(
          Long.parseLong(vertexValue[j + 1]));
      int sinkPartitionID = Integer.parseInt(vertexValue[j + 2]);
      j += 2;
      IntWritable edgeID = new IntWritable(
          edgeCount++ | (( peer.getPeerIndex()) << 27));
      Edge<E, LongWritable, IntWritable> e = new Edge<E, LongWritable, IntWritable>(
          edgeID, sinkID);
      _adjList.add(e);
      if (sinkPartitionID != peer.getPeerIndex() && subgraph.getVertexById(sinkID) == null) {
        // this is a remote vertex
        IRemoteVertex<V, E, LongWritable, IntWritable, LongWritable> sink = new RemoteVertex<>(
            sinkID, sinkSubgraphID);
        // Add it to the same subgraph, as this is part of weakly connected
        // component
        subgraph.addVertex(sink);
      }
    }
    subgraph.addVertex(createVertexInstance(vertexID, _adjList));
  }

  private IVertex<V, E, LongWritable, IntWritable> createVertexInstance(LongWritable vertexID, List<IEdge<E, LongWritable, IntWritable>> adjList) {
    return ReflectionUtils.newInstance(GraphJobRunner.VERTEX_CLASS, new Class<?>[] {Writable.class, Iterable.class},
            new Object[] {vertexID, adjList});
  }
}
