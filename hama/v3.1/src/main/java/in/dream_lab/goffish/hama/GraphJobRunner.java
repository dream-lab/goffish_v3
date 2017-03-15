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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.Combiner;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.Partitioner;
import org.apache.hama.bsp.PartitioningRunner;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.commons.util.KeyValuePair;
import org.apache.hama.util.ReflectionUtils;
import org.apache.hama.util.UnsafeByteArrayInputStream;
import org.apache.hama.util.WritableUtils;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import in.dream_lab.goffish.api.AbstractSubgraphComputation;
import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IMessage;
import in.dream_lab.goffish.api.IRemoteVertex;
import in.dream_lab.goffish.api.ISubgraph;
import in.dream_lab.goffish.api.ISubgraphCompute;
import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.hama.GraphJob;
import in.dream_lab.goffish.hama.Vertex;
import in.dream_lab.goffish.hama.api.IControlMessage;
import in.dream_lab.goffish.hama.api.IReader;
/**
 * Fully generic graph job runner.
 * 
 * @param <V> the id type of a vertex.
 * @param <E> the value type of an edge.
 * @param <M> the value type of a vertex.
 */

public final class GraphJobRunner<S extends Writable, V extends Writable, E extends Writable, M extends Writable, I extends Writable, J extends Writable, K extends Writable>
    extends BSP<Writable, Writable, Writable, Writable, Message<K, M>> {

  
  /* Maintains statistics about graph job. Updated by master. */
  public static enum GraphJobCounter {
    ACTIVE_SUBGRAPHS, VERTEX_COUNT, EDGE_COUNT , ITERATIONS
  }
  
  public static final Log LOG = LogFactory.getLog(GraphJobRunner.class);
  
  private Partition<S, V, E, I, J, K> partition;
  private BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer;
  private HamaConfiguration conf;
  private Map<K, Integer> subgraphPartitionMap;
  private static Class<?> SUBGRAPH_CLASS;
  public static Class<? extends Writable> GRAPH_MESSAGE_CLASS;
  
  //public static Class<Subgraph<?, ?, ?, ?, ?, ?, ?>> subgraphClass;
  private Map<K, List<IMessage<K, M>>> subgraphMessageMap;
  private List<SubgraphCompute<S, V, E, M, I, J, K>> subgraphs=new ArrayList<SubgraphCompute<S, V, E, M, I, J, K>>();
  boolean allVotedToHalt = false, messageInFlight = false, globalVoteToHalt = false;
  
  @Override
  public final void setup(
      BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer)
      throws IOException, SyncException, InterruptedException {

    setupfields(peer);
    
    /*TODO: Read input reader class type from Hama conf. */

    Class<? extends IReader> readerClass = conf.getClass(GraphJob.READER_CLASS_ATTR, LongTextAdjacencyListReader.class, IReader.class);
    List<Object> params = new ArrayList<Object>();
    params.add(peer);
    params.add(subgraphPartitionMap);
    Class<?> paramClasses[] = { BSPPeer.class, Map.class };

    IReader<Writable, Writable, Writable, Writable, S, V, E, I, J, K> reader = ReflectionUtils
        .newInstance(readerClass, paramClasses, params.toArray());

    for (ISubgraph<S, V, E, I, J, K> subgraph : reader.getSubgraphs()) {
      partition.addSubgraph(subgraph);
    }

  }
  
  /*Initialize the  fields*/
  @SuppressWarnings("unchecked")
  private void setupfields(
      BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer) {
    
    this.peer = peer;
    partition = new Partition<S, V, E, I, J, K>(peer.getPeerIndex());
    this.conf = peer.getConfiguration();
    this.subgraphPartitionMap = new HashMap<K, Integer>();
    this.subgraphMessageMap = new HashMap<K, List<IMessage<K, M>>>();
    // TODO : Add support for Richer Subgraph
    
    Class<M> graphMessageClass = (Class<M>) conf.getClass(
        GraphJob.GRAPH_MESSAGE_CLASS_ATTR, IntWritable.class, Writable.class);
    GRAPH_MESSAGE_CLASS = graphMessageClass;
    
  }

   
  @Override
  public final void bsp(
      BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer)
      throws IOException, SyncException, InterruptedException {
    
    //if the application needs any input in the 0th superstep
    String initialValue = conf.get(GraphJob.INITIAL_VALUE);
    /*
     * Creating SubgraphCompute objects
     */
    for (ISubgraph<S, V, E, I, J, K> subgraph : partition.getSubgraphs()) {
      
      if (initialValue != null) {
        Class<? extends AbstractSubgraphComputation<S, V, E, M, I, J, K>> subgraphComputeClass = null;
        subgraphComputeClass = (Class<? extends AbstractSubgraphComputation<S, V, E, M, I, J, K>>) conf
              .getClass(GraphJob.SUBGRAPH_COMPUTE_CLASS_ATTR, null);
        Object []params = {initialValue};
        AbstractSubgraphComputation<S, V, E, M, I, J, K> abstractSubgraphComputeRunner = ReflectionUtils
            .newInstance(subgraphComputeClass, params);
        SubgraphCompute<S, V, E, M, I, J, K> subgraphComputeRunner = new SubgraphCompute<S, V, E, M, I, J, K>();
        subgraphComputeRunner.setAbstractSubgraphCompute(abstractSubgraphComputeRunner);
        abstractSubgraphComputeRunner.setSubgraphPlatformCompute(subgraphComputeRunner);
        subgraphComputeRunner.setSubgraph(subgraph);
        subgraphComputeRunner.init(this);
        subgraphs.add(subgraphComputeRunner);
        continue;
      }
      
      Class<? extends AbstractSubgraphComputation<S, V, E, M, I, J, K>> subgraphComputeClass = null;
        subgraphComputeClass = (Class<? extends AbstractSubgraphComputation<S, V, E, M, I, J, K>>) conf
            .getClass(GraphJob.SUBGRAPH_COMPUTE_CLASS_ATTR, null);
      
      AbstractSubgraphComputation<S, V, E, M, I, J, K> abstractSubgraphComputeRunner = ReflectionUtils
          .newInstance(subgraphComputeClass);
      SubgraphCompute<S, V, E, M, I, J, K> subgraphComputeRunner = new SubgraphCompute<S, V, E, M, I, J, K>();
      subgraphComputeRunner.setAbstractSubgraphCompute(abstractSubgraphComputeRunner);
      abstractSubgraphComputeRunner.setSubgraphPlatformCompute(subgraphComputeRunner);
      subgraphComputeRunner.setSubgraph(subgraph);
      subgraphComputeRunner.init(this);
      subgraphs.add(subgraphComputeRunner);
    }
    
    //Completed all initialization steps at this point
    peer.sync();
    
    while (!globalVoteToHalt) {     
      
      LOG.info("Application SuperStep "+getSuperStepCount());
      
      subgraphMessageMap = new HashMap<K, List<IMessage<K, M>>>();
      globalVoteToHalt = (isMasterTask(peer) && getSuperStepCount() != 0) ? true : false;
      allVotedToHalt = true;
      messageInFlight = false;
      parseMessages();

      if (globalVoteToHalt && isMasterTask(peer)) {
        notifyJobEnd();
        peer.sync(); // Wait for all the peers to receive the message in next
                     // superstep.
        break;
      } else if (globalVoteToHalt) {
        break;
      }

      for (SubgraphCompute<S, V, E, M, I, J, K> subgraph : subgraphs) {
        boolean hasMessages = false;
        List<IMessage<K, M>> messagesToSubgraph = subgraphMessageMap.get(subgraph.getSubgraph().getSubgraphId());
        if (messagesToSubgraph != null) {
          hasMessages = true;
        } else {
          // if null is passed to compute it might give null pointer exception
          messagesToSubgraph = Lists.newArrayList();
        }

        if (!subgraph.hasVotedToHalt() || hasMessages) {
          subgraph.setActive();
          subgraph.compute(messagesToSubgraph);
          if (!subgraph.hasVotedToHalt())
            allVotedToHalt = false;
        }
      }
      sendHeartBeat();

      peer.getCounter(GraphJobCounter.ITERATIONS).increment(1);

      peer.sync();

    }
  }

  @Override
  public final void cleanup(
      BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer)
      throws IOException {

    for (ISubgraphCompute<S, V, E, M, I, J, K> subgraph : subgraphs) {
      System.out.println("Subgraph " + subgraph.getSubgraph().getSubgraphId()
          + " value: " + subgraph.getSubgraph().getSubgraphValue());
    }
  }

  /*
   * Each peer sends heart beat to the master, which indicates if all the
   * subgraphs has voted to halt and there is no message which is being sent in
   * the current superstep.
   */
  void sendHeartBeat() {
    Message<K, M> msg = new Message<K, M>();
    ControlMessage controlInfo = new ControlMessage();
    controlInfo.setTransmissionType(IControlMessage.TransmissionType.HEARTBEAT);
    int allVotedToHaltMsg = (allVotedToHalt) ? 1 : 0;
    int messageInFlightMsg = (messageInFlight) ? 1 : 0;
    int heartBeatMsg = allVotedToHaltMsg << 1 + messageInFlightMsg;
    controlInfo.addextraInfo(Ints.toByteArray(heartBeatMsg));
    msg.setControlInfo(controlInfo);
    sendMessage(peer.getPeerName(getMasterTaskIndex()), msg);
  }

  void parseMessages() throws IOException {

    Message<K, M> message;
    while ((message = peer.getCurrentMessage()) != null) {
      // Broadcast message, therefore every subgraph receives it
      if (((Message<K, M>) message).getControlInfo().getTransmissionType() == IControlMessage.TransmissionType.BROADCAST) {
        for (ISubgraph<S, V, E, I, J, K> subgraph : partition.getSubgraphs()) {
          List<IMessage<K, M>> subgraphMessage = subgraphMessageMap.get(subgraph.getSubgraphId());
          if (subgraphMessage == null) {
            subgraphMessage = new ArrayList<IMessage<K, M>>();
            subgraphMessageMap.put(subgraph.getSubgraphId(), subgraphMessage);
          }
          subgraphMessage.add(message);
        }
      }
      else if (((Message<K, M>) message).getControlInfo().getTransmissionType() == IControlMessage.TransmissionType.NORMAL) {
        List<IMessage<K, M>> subgraphMessage = subgraphMessageMap.get(message.getSubgraphId());
        if (subgraphMessage == null) {
          subgraphMessage = new ArrayList<IMessage<K, M>>();
          subgraphMessageMap.put(message.getSubgraphId(), subgraphMessage);
        }
        subgraphMessage.add(message);
      }
      else if (((Message<K, M>) message).getControlInfo().getTransmissionType() == IControlMessage.TransmissionType.GLOBAL_HALT) {
        globalVoteToHalt = true;
      }
      else if (((Message<K, M>) message).getControlInfo().getTransmissionType() == IControlMessage.TransmissionType.HEARTBEAT) {
        assert (isMasterTask(peer));
        parseHeartBeat(message);
      } else {
        System.out.println("Invalid transmission type!");
      }
      /*
       * TODO: Add implementation for partition message and vertex message(used
       * for graph mutation)
       */
    }
  }

  /* Sets global vote to halt to false if any of the peer is still active. */
  void parseHeartBeat(IMessage<K, M> message) {
    ControlMessage content = (ControlMessage) ((Message<K, M>) message)
        .getControlInfo();
    byte[] heartBeatRaw = content.getExtraInfo().iterator().next().getBytes();
    int heartBeat = Ints.fromByteArray(heartBeatRaw);
    if (heartBeat != 2) // Heartbeat msg = 0x10 when some subgraphs are still
                        // active
      globalVoteToHalt = false;
  }

  /* Returns true if the peer is the master task, else false. */
  boolean isMasterTask(
      BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer) {
    return (getMasterTaskIndex() == peer.getPeerIndex()) ? true : false;
  }

  /* Peer 0 is the master task. */
  int getMasterTaskIndex() {
    return 0;
  }

  /*
   * Sends message to the peer, which can later be parsed to reach the
   * destination e.g. subgraph, vertex etc. Also updates the messageInFlight
   * boolean.
   */
  void sendMessage(String peerName, Message<K, M> message) {
    try {
      peer.send(peerName, message);
      if (message.getControlInfo()
          .getTransmissionType() != IControlMessage.TransmissionType.HEARTBEAT) {
        messageInFlight = true;
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  /* Sends message to all the peers. */
  void sendToAll(Message<K, M> message) {
    for (String peerName : peer.getAllPeerNames()) {
      sendMessage(peerName, message);
    }
  }

  void sendMessage(K subgraphID, M message) {
    Message<K, M> msg = new Message<K, M>(Message.MessageType.CUSTOM_MESSAGE,
        subgraphID, message);
    ControlMessage controlInfo = new ControlMessage();
    controlInfo.setTransmissionType(IControlMessage.TransmissionType.NORMAL);
    msg.setControlInfo(controlInfo);
    sendMessage(peer.getPeerName(subgraphPartitionMap.get(subgraphID)), msg);
  }

  void sendToVertex(I vertexID, M message) {
    // TODO
  }

  void sendToNeighbors(ISubgraph<S, V, E, I, J, K> subgraph, M message) {
    Set<K> sent = new HashSet<K>();
    for (IRemoteVertex<V, E, I, J, K> remotevertices : subgraph
        .getRemoteVertices()) {
      K neighbourID = remotevertices.getSubgraphId();
      if (!sent.contains(neighbourID)) {
        sent.add(neighbourID);
        sendMessage(neighbourID, message);
      }
    }
  }

  void sendToAll(M message) {
    Message<K, M> msg = new Message<K, M>(Message.MessageType.CUSTOM_MESSAGE,
        message);
    ControlMessage controlInfo = new ControlMessage();
    controlInfo.setTransmissionType(IControlMessage.TransmissionType.BROADCAST);
    msg.setControlInfo(controlInfo);
    sendToAll(msg);
  }

  long getSuperStepCount() {
    return peer.getCounter(GraphJobCounter.ITERATIONS).getCounter();
  }

  int getPartitionID(K subgraphID) {
    return 0;
  }

  /*
   * Master task notifies all the peers to finish bsp as all the subgraphs
   * across all the peers have voted to halt and there is no message in flight.
   */
  void notifyJobEnd() {
    assert (isMasterTask(peer));
    Message<K, M> msg = new Message<K, M>();
    ControlMessage controlInfo = new ControlMessage();
    controlInfo
        .setTransmissionType(IControlMessage.TransmissionType.GLOBAL_HALT);
    msg.setControlInfo(controlInfo);
    sendToAll(msg);
  }
}
