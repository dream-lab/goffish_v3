/**
 *  Copyright 2017 DREAM:Lab, Indian Institute of Science, Bangalore
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may
 *  not use this file except in compliance with the License. You may obtain
 *  a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  
 *  @author Himanshu Sharma
 *  @author Diptanshu Kakwani
*/

package in.dream_lab.goffish.hama;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.commons.util.KeyValuePair;
import org.apache.hama.util.ReflectionUtils;

import com.google.common.primitives.Longs;

import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IRemoteVertex;
import in.dream_lab.goffish.api.ISubgraph;
import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.hama.api.IControlMessage;
import in.dream_lab.goffish.hama.api.IReader;

public class FullInfoNonSplitReader<S extends Writable, V extends Writable, E extends Writable, K extends Writable, M extends Writable>
    implements
    IReader<Writable, Writable, Writable, Writable, S, V, E, LongWritable, LongWritable, LongWritable> {

  public static final Log LOG = LogFactory.getLog(FullInfoNonSplitReader.class);

  private HamaConfiguration conf;
  private BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer;
  private Partition<S, V, E, LongWritable, LongWritable, LongWritable> partition;
  private Map<K, Integer> subgraphPartitionMap;
  private int edgeCount = 0;

  public FullInfoNonSplitReader(
      BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer,
      Map<K, Integer> subgraphPartitionMap) {
    this.peer = peer;
    this.subgraphPartitionMap = subgraphPartitionMap;
    this.conf = peer.getConfiguration();
    partition = new Partition<>(peer.getPeerIndex());
  }

  @Override
  public List<ISubgraph<S, V, E, LongWritable, LongWritable, LongWritable>> getSubgraphs()
      throws IOException, SyncException, InterruptedException {

    long startTime = System.currentTimeMillis();

    KeyValuePair<Writable, Writable> pair;
    while ((pair = peer.readNext()) != null) {
      String stringInput = pair.getValue().toString();
      createVertex(stringInput);
    }

    LOG.debug("Finished Graph Loading in partition"+peer.getPeerIndex());

    // broadcast all subgraphs belonging to this partition
    Message<K, M> subgraphMapppingMessage = new Message<>();
    subgraphMapppingMessage.setMessageType(Message.MessageType.CUSTOM_MESSAGE);
    ControlMessage controlInfo = new ControlMessage();
    controlInfo.setTransmissionType(IControlMessage.TransmissionType.BROADCAST);
    controlInfo.setPartitionID(peer.getPeerIndex());
    subgraphMapppingMessage.setControlInfo(controlInfo);
    for (ISubgraph<S, V, E, LongWritable, LongWritable, LongWritable> subgraph : partition
        .getSubgraphs()) {

      byte subgraphIDbytes[] = Longs
          .toByteArray(subgraph.getSubgraphId().get());
      controlInfo.addextraInfo(subgraphIDbytes);
    }

    sendToAllPartitions(subgraphMapppingMessage);
    
    LOG.debug("Subgraph partition Broadcast sent");
    long endTime = System.currentTimeMillis();
    LOG.info("GOFFISH3.PERF.GRAPH_LOAD," + peer.getPeerIndex() + "," + peer.getSuperstepCount() +
            "," + startTime + "," + endTime + "," + (endTime - startTime));

    peer.sync();

    startTime = System.currentTimeMillis();

    Message<K, M> subgraphMappingInfoMessage;
    while ((subgraphMappingInfoMessage = peer.getCurrentMessage()) != null) {
      ControlMessage receivedCtrl = (ControlMessage) subgraphMappingInfoMessage
          .getControlInfo();
      Integer partitionID = receivedCtrl.getPartitionID();
      for (BytesWritable rawSubgraphID : receivedCtrl.getExtraInfo()) {
        LongWritable subgraphID = new LongWritable(
            Longs.fromByteArray(rawSubgraphID.copyBytes()));
        subgraphPartitionMap.put((K) subgraphID, partitionID);
      }
    }
    LOG.debug("Reader Completed");
    endTime = System.currentTimeMillis();
    LOG.info("GOFFISH3.PERF.GRAPH_LOAD," + peer.getPeerIndex() + "," + peer.getSuperstepCount() +
            "," + startTime + "," + endTime + "," + (endTime - startTime));

    return partition.getSubgraphs();
  }

  private void sendToAllPartitions(Message<K, M> message) throws IOException {
    for (String peerName : peer.getAllPeerNames()) {
      peer.send(peerName, message);
    }
  }

  private void createVertex(String stringInput) {

    String vertexValue[] = stringInput.split("\\s+");

    LongWritable vertexID = new LongWritable(Long.parseLong(vertexValue[1]));
    int partitionID = Integer.parseInt(vertexValue[0]);
    LongWritable vertexSubgraphID = new LongWritable(
        Long.parseLong(vertexValue[2]));

    Subgraph<S, V, E, LongWritable, LongWritable, LongWritable> subgraph = (Subgraph<S, V, E, LongWritable, LongWritable, LongWritable>) partition
        .getSubgraph(vertexSubgraphID);

    if (subgraph == null) {
      subgraph = new Subgraph<S, V, E, LongWritable, LongWritable, LongWritable>(
          partitionID, vertexSubgraphID);
      partition.addSubgraph(subgraph);
    }
    List<IEdge<E, LongWritable, LongWritable>> _adjList = new ArrayList<IEdge<E, LongWritable, LongWritable>>();

    for (int j = 3; j < vertexValue.length; j++) {
      if (j + 3 > vertexValue.length) {
        LOG.debug("Incorrect length of line for vertex " + vertexID);
      }
      LongWritable sinkID = new LongWritable(Long.parseLong(vertexValue[j]));
      LongWritable sinkSubgraphID = new LongWritable(
          Long.parseLong(vertexValue[j + 1]));
      int sinkPartitionID = Integer.parseInt(vertexValue[j + 2]);
      j += 2;
      LongWritable edgeID = new LongWritable(
          edgeCount++ | (((long) peer.getPeerIndex()) << 32));
      Edge<E, LongWritable, LongWritable> e = new Edge<E, LongWritable, LongWritable>(
          edgeID, sinkID);
      _adjList.add(e);
      if (sinkPartitionID != peer.getPeerIndex()
          && subgraph.getVertexById(sinkID) == null) {
        // this is a remote vertex
        IRemoteVertex<V, E, LongWritable, LongWritable, LongWritable> sink = new RemoteVertex<>(
            sinkID, sinkSubgraphID);
        // Add it to the same subgraph, as this is part of weakly connected
        // component
        subgraph.addVertex(sink);
      }
    }
    subgraph.addVertex(createVertexInstance(vertexID, _adjList));
  }

  private IVertex<V, E, LongWritable, LongWritable> createVertexInstance(
      LongWritable vertexID,
      List<IEdge<E, LongWritable, LongWritable>> adjList) {
    return ReflectionUtils.newInstance(GraphJobRunner.VERTEX_CLASS,
        new Class<?>[] { Writable.class, Iterable.class },
        new Object[] { vertexID, adjList });
  }
}
