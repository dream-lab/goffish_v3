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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Iterables;
import in.dream_lab.goffish.api.*;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.Combiner;
import org.apache.hama.bsp.Partitioner;
import org.apache.hama.bsp.PartitioningRunner;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.util.ReflectionUtils;

import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;

import in.dream_lab.goffish.api.ISubgraphWrapup;
import in.dream_lab.goffish.hama.GraphJob;
import in.dream_lab.goffish.hama.Vertex;
import in.dream_lab.goffish.hama.api.IControlMessage;
import in.dream_lab.goffish.hama.api.IReader;
/**
 * Fully generic graph job runner.
 *
 * @param <S> Subgraph value object type
 * @param <V> Vertex value object type
 * @param <E> Edge value object type
 * @param <M> Message object type
 * @param <I> Vertex ID type
 * @param <J> Edge ID type
 * @param <K> Subgraph ID type
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
  public static Class<? extends IVertex> VERTEX_CLASS;
  private static int THREAD_COUNT;
  
  //public static Class<Subgraph<?, ?, ?, ?, ?, ?, ?>> subgraphClass;
  private Map<K, List<IMessage<K, M>>> subgraphMessageMap;
  private List<SubgraphCompute<S, V, E, M, I, J, K>> subgraphs=new ArrayList<SubgraphCompute<S, V, E, M, I, J, K>>();
  boolean allVotedToHalt = false, messageInFlight = false, globalVoteToHalt = false;

  // Stats for logging application level messages
  long sgMsgRecv, broadcastMsgRecv, sgMsgSend, broadcastMsgSend;

  // Track memory usage
  Runtime runtime = Runtime.getRuntime();
  long mb = 1024 * 1024;

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public final void setup(
      BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer)
      throws IOException, SyncException, InterruptedException {

    setupfields(peer);
    
    Class<? extends IReader> readerClass = conf.getClass(GraphJob.READER_CLASS_ATTR, LongTextAdjacencyListReader.class, IReader.class);
    List<Object> params = new ArrayList<Object>();
    params.add(peer);
    params.add(subgraphPartitionMap);
    Class<?> paramClasses[] = { BSPPeer.class, Map.class };

    IReader<Writable, Writable, Writable, Writable, S, V, E, I, J, K> reader = ReflectionUtils
        .newInstance(readerClass, paramClasses, params.toArray());

    long startTime = System.currentTimeMillis();
    List<ISubgraph<S, V, E, I, J, K>> subgraphs = reader.getSubgraphs();
    long endTime = System.currentTimeMillis();
    LOG.info("PERF.GRAPH_LOAD," + peer.getPeerIndex() + "," + startTime + "," +
             endTime + "," + (endTime - startTime));
    LOG.info("WORKER.INFO," + peer.getPeerIndex() + "," + peer.getPeerName() + ","
             + peer.getNumPeers() + "," + (Runtime.getRuntime().availableProcessors() * 2));

    for (ISubgraph<S, V, E, I, J, K> subgraph : subgraphs) {
      partition.addSubgraph(subgraph);

      // Logging info about subgraph
      if (LOG.isInfoEnabled()) {
        long localEdgeCount = 0, remoteEdgeCount = 0, boundaryVertexCount = 0, maxEdgeDegree = 0;
        K sgid = subgraph.getSubgraphId();
        long localVertexCount = subgraph.getLocalVertexCount();
        long remoteVertexCount = Iterables.size(subgraph.getRemoteVertices());
        Map<K, Long> neighborSgId = new HashMap<K, Long>();

        for (IVertex<V, E, I, J> v : subgraph.getLocalVertices()) {
          boolean isBoundary = false;
          long edgeDegree = 0;
          for (IEdge<E, I, J> e : v.getOutEdges()) {
            edgeDegree++;
            IVertex<V, E, I, J> adjVertex = subgraph.getVertexById(e.getSinkVertexId());
            if (adjVertex.isRemote()) {
              isBoundary = true;
              remoteEdgeCount++;
              K adjSgid = ((IRemoteVertex<V, E, I, J, K>)adjVertex).getSubgraphId();
              Long remoteEdgesSg = neighborSgId.get(adjSgid);
              if (remoteEdgesSg == null)
                remoteEdgesSg = new Long(0);
              neighborSgId.put(adjSgid, remoteEdgesSg + 1);
            } else
              localEdgeCount++;
          }
          if (isBoundary)
            boundaryVertexCount++;
          if (edgeDegree > maxEdgeDegree)
            maxEdgeDegree = edgeDegree;
        }

        LOG.info("TOPO.SG," + sgid + "," + localVertexCount + "," + localEdgeCount + ","
                 + remoteVertexCount + "," + remoteEdgeCount + "," + boundaryVertexCount
                 + "," + maxEdgeDegree);

        String adjSgidString = new String(), prefix = "";
        for (Map.Entry<K, Long> e : neighborSgId.entrySet()) {
          adjSgidString += prefix + e.getKey() + "," + e.getValue();
          prefix = ",";
        }
        if (adjSgidString.isEmpty())
          LOG.info("META.SG," + sgid + "," + localVertexCount);
        else
          LOG.info("META.SG," + sgid + "," + localVertexCount + "," + adjSgidString);
      }
    }


  }
  
  /*Initialize the  fields*/
  @SuppressWarnings({ "unchecked", "rawtypes" })
  private void setupfields(
      BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer) {
    
    this.peer = peer;
    partition = new Partition<S, V, E, I, J, K>(peer.getPeerIndex());
    this.conf = peer.getConfiguration();
    this.subgraphPartitionMap = new HashMap<K, Integer>();
    this.subgraphMessageMap = new HashMap<K, List<IMessage<K, M>>>();
    this.THREAD_COUNT = 2 * Runtime.getRuntime().availableProcessors();
    // TODO : Add support for Richer Subgraph
    
    Class<M> graphMessageClass = (Class<M>) conf.getClass(
        GraphJob.GRAPH_MESSAGE_CLASS_ATTR, IntWritable.class, Writable.class);
    GRAPH_MESSAGE_CLASS = graphMessageClass;

    Class<? extends IVertex> vertexClass = (Class<? extends IVertex>) conf.getClass(
            GraphJob.VERTEX_CLASS_ATTR, Vertex.class, IVertex.class);
    VERTEX_CLASS = vertexClass;
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

      LOG.info("Application SuperStep " + getSuperStepCount());
      LOG.info("PERF.SS_MEM," + ((runtime.totalMemory() - runtime.freeMemory()) / mb) + ","
               + (runtime.freeMemory() / mb) + "," +  (runtime.totalMemory() / mb) + ","
               + (runtime.maxMemory() / mb));
      
      subgraphMessageMap = new HashMap<K, List<IMessage<K, M>>>();
      globalVoteToHalt = (isMasterTask(peer) && getSuperStepCount() != 0) ? true : false;
      allVotedToHalt = true;
      messageInFlight = false;
      sgMsgRecv = broadcastMsgRecv = sgMsgSend = broadcastMsgSend = 0;
      parseMessages();

      if (globalVoteToHalt && isMasterTask(peer)) {
        notifyJobEnd();
        peer.sync(); // Wait for all the peers to receive the message in next
                     // superstep.
        break;
      } else if (globalVoteToHalt) {
        break;
      }
      
      ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors
          .newFixedThreadPool(THREAD_COUNT);
      //executor.setMaximumPoolSize(Runtime.getRuntime().availableProcessors()*2);
      //executor.setRejectedExecutionHandler(retryHandler);
      
      long subgraphsExecutedThisSuperstep = 0;

      for (SubgraphCompute<S, V, E, M, I, J, K> subgraph : subgraphs) {
        boolean hasMessages = false;
        List<IMessage<K, M>> messagesToSubgraph = subgraphMessageMap.get(subgraph.getSubgraph().getSubgraphId());
        if (messagesToSubgraph != null) {
          hasMessages = true;
        } else {
          // if null is passed to compute it might give null pointer exception
          messagesToSubgraph = Lists.newArrayList();
        }

        LOG.info("PERF.SG.RECV_MSG_COUNT," + subgraph.getSubgraph().getSubgraphId() + "," + getSuperStepCount()
                + "," + sgMsgRecv + "," + broadcastMsgRecv + "," + (sgMsgRecv + broadcastMsgRecv));

        if (!subgraph.hasVotedToHalt() || hasMessages) {
          executor.execute(new ComputeRunnable(subgraph, messagesToSubgraph));
          subgraphsExecutedThisSuperstep++;
        }
        else {
          long curTime = System.currentTimeMillis();
          LOG.info("PERF.SG.COMPUTE_TIME," + subgraph.getSubgraph().getSubgraphId() + ","
                  + getSuperStepCount() + "," + curTime + "," + curTime + "," + 0);
        }

        LOG.info("PERF.SG.SEND_MSG_COUNT," + subgraph.getSubgraph().getSubgraphId() + "," + getSuperStepCount()
                 + "," + sgMsgSend + "," + broadcastMsgSend + "," + (sgMsgSend + broadcastMsgSend));
      }

      executor.shutdown();

      while (!executor.awaitTermination(5, TimeUnit.SECONDS))
        System.out.println(
            "Waiting. Submitted tasks: " + subgraphsExecutedThisSuperstep
                + ". Completed threads: " + executor.getCompletedTaskCount());

      if (executor.getCompletedTaskCount() != subgraphsExecutedThisSuperstep)
        System.out.println("ERROR: expected " + subgraphsExecutedThisSuperstep
            + " but found only completed threads "
            + executor.getCompletedTaskCount());

      // executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

      sendHeartBeat();

      peer.getCounter(GraphJobCounter.ITERATIONS).increment(1);

      peer.sync();

    }
  }

  @Override
  public final void cleanup(
      BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer)
      throws IOException {

    for (SubgraphCompute<S, V, E, M, I, J, K> subgraph : subgraphs) {
      AbstractSubgraphComputation<S, V, E, M, I, J, K> abstractSubgraphCompute = subgraph
          .getAbstractSubgraphCompute();
      if (abstractSubgraphCompute instanceof ISubgraphWrapup) {
        ((ISubgraphWrapup) abstractSubgraphCompute).wrapup();
      }
      System.out.println("Subgraph " + subgraph.getSubgraph().getSubgraphId()
          + " value: " + subgraph.getSubgraph().getSubgraphValue());
    }
  }
  
  
  class ComputeRunnable implements Runnable {
    SubgraphCompute<S, V, E, M, I, J, K> subgraphComputeRunner;
    Iterable<IMessage<K, M>> msgs;

    @SuppressWarnings("unchecked")
    public ComputeRunnable(SubgraphCompute<S, V, E, M, I, J, K> runner, List<IMessage<K, M>> messages) throws IOException {
      this.subgraphComputeRunner = runner;
      this.msgs = messages;
    }

    @Override
    public void run() {
      try {
        // call once at initial superstep
        /*
        if (getSuperStepCount() == 0) { subgraphComputeRunner.setup(conf);
        msgs = Collections.singleton(subgraphComputeRunner.getSubgraph().
        getSubgraphValue()); }
         */
        subgraphComputeRunner.setActive();
        long startTime = System.currentTimeMillis();
        subgraphComputeRunner.compute(msgs);
        long endTime = System.currentTimeMillis();
        LOG.info("PERF.SG.COMPUTE_TIME," + subgraphComputeRunner.getSubgraph().getSubgraphId() + ","
                + getSuperStepCount() + "," + startTime + "," + endTime + "," + (endTime - startTime));
        if (!subgraphComputeRunner.hasVotedToHalt())
          allVotedToHalt = false;

      } catch (IOException e) {
        throw new RuntimeException(e);
      }
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
        broadcastMsgRecv++;
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
        sgMsgRecv++;
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
  private void sendMessage(String peerName, Message<K, M> message) {
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
  private void sendToAll(Message<K, M> message) {
    for (String peerName : peer.getAllPeerNames()) {
      sendMessage(peerName, message);
    }
  }

  synchronized void sendMessage(K subgraphID, M message) {
    sgMsgSend++;
    Message<K, M> msg = new Message<K, M>(Message.MessageType.CUSTOM_MESSAGE,
        subgraphID, message);
    ControlMessage controlInfo = new ControlMessage();
    controlInfo.setTransmissionType(IControlMessage.TransmissionType.NORMAL);
    msg.setControlInfo(controlInfo);
    sendMessage(peer.getPeerName(subgraphPartitionMap.get(subgraphID)), msg);
  }

  synchronized void sendToVertex(I vertexID, M message) {
    // TODO
  }

  synchronized void sendToNeighbors(ISubgraph<S, V, E, I, J, K> subgraph, M message) {
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

  synchronized void sendToAll(M message) {
    broadcastMsgSend++;
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
