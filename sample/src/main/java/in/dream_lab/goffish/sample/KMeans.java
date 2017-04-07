package in.dream_lab.goffish.sample;

import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IMessage;
import in.dream_lab.goffish.api.AbstractSubgraphComputation;;
import in.dream_lab.goffish.api.IRemoteVertex;
import in.dream_lab.goffish.api.IVertex;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;


/** Based on the GPS implementation of K-Means (http://infolab.stanford.edu/gps/gps_tr.pdf).
 *
 *
 *
 * @author Diptanshu Kakwani
 * @version 1.0
 * @see <a href="http://www.dream-lab.in/">DREAM:Lab</a>
 *
 * Copyright 2014 DREAM:Lab, Indian Institute of Science, Bangalore
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class KMeans extends
    AbstractSubgraphComputation<LongWritable, LongWritable, LongWritable, Text, LongWritable, LongWritable, LongWritable> {

  public static final Log LOG = LogFactory.getLog(KMeans.class);

  Set<Long> clusterCenters;                     // Assigned cluster center in this subgraph
  Set<Long> localUpdated, remoteUpdated;        // Local and remote vertices which have been updated
  long k = 100;                                                         // The required number of clusters in the whole graph
  Map<Long, Pair<Long, Long>> dist;     // Shortest distance of each node and its assigned cluster ID
  List<Pair<Double, Long>> possibleCenters;
  PriorityQueue<Pair<Double, Long>> allCenters;
  long edgeCrossing, maxEdgeCrossing = 1;
  int maxIterations = 10, iterations;
  long minSubgraphId = Long.MAX_VALUE;


  enum Phase {PICK_LOCAL_CLUSTER, PICK_GLOBAL_CLUSTER, CLUSTER_ASSIGNMENT, EDGE_CROSSING,
    SEND_EDGE_CROSSING, FINISHED};
  Phase phase = Phase.PICK_LOCAL_CLUSTER;
  boolean clusterAssigned, startNextPhase;


  // To keep track of minimum distance and the cluster id
  private class Pair<L, R> {
    L first;
    R second;

    Pair(L a, R b) {
      first = a;
      second = b;
    }
  }

  class PairComparator implements Comparator<Pair<Double, Long>> {

    @Override
    public int compare(Pair<Double, Long> arg0, Pair<Double, Long> arg1) {
      return arg0.first.compareTo(arg1.first);
    }

  }

  public KMeans(String initMsg) {
    String[] inp = initMsg.split(",");
    k = Long.parseLong(inp[0]);
    maxEdgeCrossing = Long.parseLong(inp[1]);
    maxIterations = Integer.parseInt(inp[2]);
  }

  @Override
  public void compute(Iterable<IMessage<LongWritable, Text>> messageList) {

    LOG.info("current phase : " + phase);
    LOG.info("Superstep " + getSuperstep() + " subgraph " + getSubgraph().getSubgraphId());
    if (phase == Phase.PICK_LOCAL_CLUSTER) {
      init();
      localCenters();
      broadcastCenters(possibleCenters);
      phase = Phase.PICK_GLOBAL_CLUSTER;
    }
    else if (phase == Phase.PICK_GLOBAL_CLUSTER) {
      if (messageList.iterator().hasNext())   // Only if we have messages, accumulate all the centers.
        accumulateAllCenters(messageList);

      kCenters(allCenters);
      remoteUpdated = localBfs(clusterCenters);
      packAndSendMessages(remoteUpdated);
      phase = Phase.CLUSTER_ASSIGNMENT;
    }
    else if (phase == Phase.CLUSTER_ASSIGNMENT) {
      startNextPhase = (getSubgraph().getSubgraphId().get() == minSubgraphId);
      localUpdated = readMessages(messageList);
      if (startNextPhase && (getSubgraph().getSubgraphId().get() == minSubgraphId) && !clusterAssigned) {
        notifyPhaseChange();
      }
      else if (startNextPhase) {
        phase = Phase.EDGE_CROSSING;
      }
      else {
        remoteUpdated = localBfs(localUpdated);
        packAndSendMessages(remoteUpdated);
      }
    }

    else if (phase == Phase.EDGE_CROSSING) {
      Map<Long, StringBuilder> neighborClusters = calculateEdgeCrossing();
      sendClusterIds(neighborClusters);
      phase = Phase.SEND_EDGE_CROSSING;
    }
    else if (phase == Phase.SEND_EDGE_CROSSING) {
      readClusterIds(messageList);
      sendEdgeCrossing();
      phase = Phase.FINISHED;
    }
    else {
      if (sumEdgeCrossings(messageList)) {
        phase = Phase.PICK_GLOBAL_CLUSTER;
        init();
        return;
      }
    }

  }

  void init() {
    minSubgraphId = 0;  // Since there always exist a BSPPeer with ID 0 and within that
                        // one subgraph with subgraph ID 0.
    iterations++;
    dist = new HashMap<Long, Pair<Long, Long>>((int)getSubgraph().getVertexCount());
    for (IVertex<LongWritable, LongWritable, LongWritable, LongWritable> v : getSubgraph().getVertices()) {
      dist.put(v.getVertexId().get(), new Pair<Long, Long>(Long.MAX_VALUE, -1l));
    }
    clusterCenters = new HashSet<Long>((int)k);
    clusterAssigned = false;
  }

  /*  Selects at most k local centers from the subgraph.
    * Based on the Full Scan algorithm from https://eyalsch.wordpress.com/2010/04/01/random-sample/
    * Iterates through the list of local vertices once and selects each item with a probability =
    * (no. of remaining to select)/(no. of remaining to scan). It guarantees that exactly
    * k vertices are selected and each subset (of size = k) of the list is equally likely.
    * In the case when k >= no. of local vertices, we choose all the vertices as centers.
  */
  void localCenters() {
    long localVertexCount = getSubgraph().getLocalVertexCount();
    possibleCenters = new ArrayList<Pair<Double, Long>>((int) k);
    Random randReservoir = new Random();

    if (k >= localVertexCount) {
      for (IVertex<LongWritable, LongWritable, LongWritable, LongWritable> v : getSubgraph().getVertices()) {
        if (!v.isRemote()) {
          LOG.info("Local center: (K >= V) vertex " + v.getVertexId() + " subgraph " + getSubgraph().getSubgraphId());
          possibleCenters.add(new Pair<Double, Long>(randReservoir.nextDouble(), v.getVertexId().get()));
        }
      }
    }
    else {
      Random rand = new Random();
      long itemsToSelect = k, visited = 0;
      Iterator<IVertex<LongWritable, LongWritable, LongWritable, LongWritable>> it
          = getSubgraph().getLocalVertices().iterator();

      while (itemsToSelect > 0) {
        IVertex<LongWritable, LongWritable, LongWritable, LongWritable> v = it.next();
        if (rand.nextDouble() < ((double)itemsToSelect)/(localVertexCount - visited)) {
          LOG.info("Local center: vertex " + v.getVertexId() + " subgraph " + getSubgraph().getSubgraphId());
          possibleCenters.add(new Pair<Double, Long>(randReservoir.nextDouble(), v.getVertexId().get()));
          itemsToSelect--;
        }
        visited++;
      }
    }
  }

  /* Broadcasts the selected local centers along with their keys to every partition. */
  void broadcastCenters(List<Pair<Double, Long>> possibleCenters) {
    StringBuilder msg = new StringBuilder();
    for (Pair<Double, Long> center : possibleCenters) {
      msg.append(center.first).append(' ').append(center.second).append(';');
    }
    sendToAll(new Text(msg.toString()));
  }

  // Accumulates all the possible centers in one priority queue.
  void accumulateAllCenters(Iterable<IMessage<LongWritable, Text>> messageList) {
    allCenters = new PriorityQueue<Pair<Double, Long>>
        (4 * (int)k, new PairComparator());   // A rough estimate of no. of overall centers =  4 * k

    for (IMessage<LongWritable, Text> messageItem : messageList) {
      String[] message = messageItem.getMessage().toString().split(";");
      for (int i = 0; i < message.length; i++) {
        double key = Double.parseDouble(message[i].split(" ")[0]);
        long vertexId = Long.parseLong(message[i].split(" ")[1]);
        allCenters.add(new Pair<Double, Long>(key, vertexId));
      }
    }
  }

  /*  Selects k global centers deterministically from the received centers.
    * Based on Distributed Reservoir Sampling from https://en.wikipedia.org/wiki/Reservoir_sampling
  */
  void kCenters(PriorityQueue<Pair<Double, Long>> allCenters) {
    clusterCenters = new HashSet<Long>();
    while (clusterCenters.size() < k) {
      Pair<Double, Long> center = allCenters.poll();
      LOG.info("Selecting " + center.second + " key " + center.first);
      clusterCenters.add(center.second);
      IVertex<LongWritable, LongWritable, LongWritable, LongWritable> v =
          getSubgraph().getVertexById(new LongWritable(center.second));
      if (v != null && !v.isRemote()) {
        dist.put(center.second, new Pair<Long, Long>(0l, center.second));
      }
    }
  }

  /* Multi-source BFS.*/
  Set<Long> localBfs(Set<Long> sourceVertices) {
    Queue<Long> Q = new ArrayDeque<Long>(sourceVertices);

    Set<Long> remoteUpdated = new HashSet<Long>();
    while (!Q.isEmpty()) {
      long id = Q.remove();
      IVertex<LongWritable, LongWritable, LongWritable, LongWritable> source =
          getSubgraph().getVertexById(new LongWritable(id));
      if (source == null || source.isRemote())
        continue;
      Pair<Long, Long> sourceDist = dist.get(id);
      LOG.info("Starting BFS from " + id + " with cluster id " + sourceDist.second +
          " in subgraph " + getSubgraph().getSubgraphId() + " having local vertices " + getSubgraph().getLocalVertexCount()
       + " remote vertices " + (getSubgraph().getVertexCount() - getSubgraph().getLocalVertexCount()));

      for (IEdge<LongWritable, LongWritable, LongWritable> edge : source.getOutEdges()) {
        IVertex<LongWritable, LongWritable, LongWritable, LongWritable> adjVertex =
            getSubgraph().getVertexById(edge.getSinkVertexId());
        Pair<Long, Long> adjDist = dist.get(adjVertex.getVertexId().get());
        if (adjDist.first > sourceDist.first + 1) {
          dist.put(adjVertex.getVertexId().get(), new Pair<Long, Long>(sourceDist.first + 1, sourceDist.second));
          if (adjVertex.isRemote())
            remoteUpdated.add(adjVertex.getVertexId().get());
          else {
            if (!Q.contains(adjVertex.getVertexId().get()))
              Q.add(adjVertex.getVertexId().get());
          }
        }
      }
    }
    return remoteUpdated;
  }

  /* Packs the message in the form: "targetVertexId minDistance assignedClusterId;" and sends them. */
  void packAndSendMessages(Set<Long> remoteUpdated) {
    if (remoteUpdated.isEmpty()) {
      sendMessage(new LongWritable(0), new Text("0"));
      return;
    }
    else {
      sendMessage(new LongWritable(0), new Text("1"));
    }

    Map<Long, StringBuilder> msg = new HashMap<Long, StringBuilder>();
    for (Long remoteVertexId : remoteUpdated) {
      Pair<Long, Long> remoteDist = dist.get(remoteVertexId);
      IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable> remoteVertex =
          (IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable>)
              getSubgraph().getVertexById(new LongWritable(remoteVertexId));
      StringBuilder content = msg.get(remoteVertex.getSubgraphId().get());
      if (content == null) {
        content = new StringBuilder();
        msg.put(remoteVertex.getSubgraphId().get(), content);
      }
      content.append(remoteVertexId).append(' ').append(remoteDist.first).append(' ').
          append(remoteDist.second).append(';');
    }
    for (Map.Entry<Long, StringBuilder> entry : msg.entrySet()) {
      sendMessage(new LongWritable(entry.getKey()), new Text(entry.getValue().toString()));
    }
  }

  /* Unpacks the messages and updates the distance if it is better than the current distance. */
  Set<Long> readMessages(Iterable<IMessage<LongWritable, Text>> messageList) {
    String[] messages;
    Set<Long> localUpdated = new HashSet<Long>();
    String x = new String();
    for (IMessage<LongWritable, Text> messageItem : messageList) {
      String msg = messageItem.getMessage().toString();
      if (msg.length() == 1) {
        x += msg;
        if (getSubgraph().getSubgraphId().get() == minSubgraphId) {
          if (msg.equals("1"))
            startNextPhase = false;
        }
        if (msg.equals("C"))
          startNextPhase = true;
        continue;
      }
      messages = new String(msg).split(";");
      for (String message : messages) {
        String[] m = message.split(" ");
        Long targetId = Long.parseLong(m[0]);
        Long minDist = Long.parseLong(m[1]);
        Long clusterId = Long.parseLong(m[2]);
        Pair<Long, Long> prevDist = dist.get(targetId);
        if (minDist < prevDist.first) {
          dist.put(targetId, new Pair<Long, Long>(minDist, clusterId));
          localUpdated.add(targetId);
        }
      }
    }
    return localUpdated;
  }

  void notifyPhaseChange() {
    clusterAssigned = true;
    sendToAll(new Text(new String("C")));
  }

  // Edge crossing is counted for each edge, so it will be double in case of undirected graphs.
  Map<Long, StringBuilder> calculateEdgeCrossing() {
    edgeCrossing = 0;
    Map<Long, StringBuilder> neighborClusters = new HashMap<Long, StringBuilder>();
    for (IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex : getSubgraph().getVertices()) {
      if (!vertex.isRemote()) {
        long clusterId = dist.get(vertex.getVertexId().get()).second;
        for (IEdge<LongWritable, LongWritable, LongWritable> edge : vertex.getOutEdges()) {
          IVertex<LongWritable, LongWritable, LongWritable, LongWritable> adjVertex =
              getSubgraph().getVertexById(edge.getSinkVertexId());

          if (adjVertex.isRemote()) {
            long remoteSubgaphId = ((IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable>)
                adjVertex).getSubgraphId().get();
            StringBuilder localClusterIds = neighborClusters.get(remoteSubgaphId);
            if (localClusterIds == null) {
              localClusterIds = new StringBuilder();
              neighborClusters.put(remoteSubgaphId, localClusterIds);
            }
            localClusterIds.append(adjVertex.getVertexId()).append(' ').append(clusterId).append(' ');
          }
          else if (!adjVertex.isRemote() && clusterId != dist.get(adjVertex.getVertexId().get()).second) {
            edgeCrossing++; // Will double count in case of undirected graph.
          }
        }
      }
    }
    return neighborClusters;
  }

  void sendClusterIds(Map<Long, StringBuilder> neighborClusters) {
    for (Map.Entry<Long, StringBuilder> e : neighborClusters.entrySet()) {
      sendMessage(new LongWritable(e.getKey()), new Text(e.getValue().toString()));
    }
  }

  void readClusterIds(Iterable<IMessage<LongWritable, Text>> messageList) {
    String[] message = null;
    for (IMessage<LongWritable, Text> messageItem : messageList) {
      message = messageItem.getMessage().toString().split(" ");
      for (int i = 0; i < message.length; i+=2) {
        long targetId = Long.parseLong(message[i]);
        long clusterId = dist.get(targetId).second;
        long neighborClusterId = Long.parseLong(message[i + 1]);
        if (clusterId != neighborClusterId)
          edgeCrossing++;
      }
    }
  }

  void sendEdgeCrossing(){
    localCenters();
    StringBuilder msg = new StringBuilder();
    msg.append(getSubgraph().getSubgraphId()).append(' ');
    msg.append(edgeCrossing).append(' ');
    for (Pair<Double, Long> p : possibleCenters) {
      msg.append(p.first).append(';').append(p.second).append(' ');
    }
    Text m = new Text(msg.toString());
    sendToAll(m);
  }

  boolean sumEdgeCrossings(Iterable<IMessage<LongWritable, Text>> messageList) {
    edgeCrossing = 0; // Avoid over-counting own edge crossing count.
    for (IMessage<LongWritable, Text> messageItem : messageList) {
      String msg = messageItem.getMessage().toString();
      int startPos = msg.indexOf(' ');
      if (Long.parseLong(msg.substring(0, startPos)) != getSubgraph().getSubgraphId().get())
        edgeCrossing += Long.parseLong(msg.substring(startPos + 1, msg.indexOf(' ', startPos + 1)));
    }
    LOG.info("Edge crossing " + edgeCrossing);
    if (!nextIteration()) {
      try {
        File file = new File("/home/humus/kmeans/cluster_id" + getSubgraph().getSubgraphId());
        PrintWriter writer = new PrintWriter(file);

        for (Map.Entry<Long, Pair<Long, Long>> e : dist.entrySet()) {
          if (!(getSubgraph().getVertexById(new LongWritable(e.getKey())).isRemote()))
            writer.println(e.getKey() + " " + e.getValue().second);
        }
        writer.flush();
        writer.close();
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      }
      return false;
    }
    else {
      LOG.info("Termination failed");
      String messages[];
      for (IMessage<LongWritable, Text> messageItem : messageList) {
        String msg = messageItem.getMessage().toString();
        messages = new String(msg).split(" ");
        for (int i = 2; i < messages.length; i++) {
          double key = Double.parseDouble(messages[i].split(";")[0]);
          long vertexId = Long.parseLong(messages[i].split(";")[1]);
          allCenters.add(new Pair<Double, Long>(key, vertexId));
        }
      }
      return true;
    }
  }

  boolean nextIteration() {
    LOG.info("Checking termination");
    if (edgeCrossing <= maxEdgeCrossing || iterations >= maxIterations)
      voteToHalt();

    return !hasVotedToHalt();
  }
}