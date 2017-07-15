package in.dream_lab.goffish.sample;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import in.dream_lab.goffish.api.AbstractSubgraphComputation;
import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IMessage;
import in.dream_lab.goffish.api.IRemoteVertex;
import in.dream_lab.goffish.api.ISubgraph;
import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.sample.ByteArrayHelper.Reader;
import in.dream_lab.goffish.sample.ByteArrayHelper.Writer;


/**
 * Based on the GPS implementation of K-Means
 * (http://infolab.stanford.edu/gps/gps_tr.pdf).
 *
 *
 *
 * @author Diptanshu Kakwani
 * @author Yogesh Simmhan
 * @version 1.0
 * @see <a href="http://www.dream-lab.in/">DREAM:Lab</a>
 *
 *      Copyright 2014 DREAM:Lab, Indian Institute of Science, Bangalore
 *
 *      Licensed under the Apache License, Version 2.0 (the "License"); you may
 *      not use this file except in compliance with the License. You may obtain
 *      a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 */

public class KMeans extends
    AbstractSubgraphComputation<KMeans.SubgraphValue, LongWritable, LongWritable, BytesWritable, LongWritable, LongWritable, LongWritable> {

        public static final Log LOG = LogFactory.getLog(KMeans.class);
        SubgraphValue value = new SubgraphValue();

        public static class SubgraphValue implements Writable {
                int k; // The required number of clusters in the whole graph

                // For each local vertex, maintains the shortest distance to its assigned
                // cluster, and the central vertex ID of that cluster
                Map<Long, Pair<Integer, Long>> dist;

                long edgeCrossings = 0;
                long maxEdgeCrossing;

                int iterations = 0;
                int maxIterations;

                long minSubgraphId;

                Phase phase = Phase.PICK_LOCAL_CLUSTER;
                boolean clusterAssigned, startNextPhase;

                @Override
                public void write(DataOutput out) throws IOException {
                        // TODO Auto-generated method stub

                }

                @Override
                public void readFields(DataInput in) throws IOException {
                        // TODO Auto-generated method stub

                }
        }

        enum Phase {
                PICK_LOCAL_CLUSTER, PICK_GLOBAL_CLUSTER, CLUSTER_ASSIGNMENT, EDGE_CROSSING, SEND_EDGE_CROSSING, FINISHED
        };


        // To keep track of minimum distance and the cluster id
        private static class Pair<L, R> {
                L first;
                R second;

                Pair(L a, R b) {
                        first = a;
                        second = b;
                }
        }

        private static class PairComparator implements Comparator<Pair<Float, Long>> {

                @Override
                public int compare(Pair<Float, Long> arg0, Pair<Float, Long> arg1) {
                        return (arg0.first.compareTo(arg1.first) == 0) ? arg0.second.compareTo(arg1.second)
                            : arg0.first.compareTo(arg1.first);
                }

        }

        /**
         * Input has <num_clusters>,<max_edge_cuts>,<max_iters>
         *
         * @param initMsg
         */
        public KMeans(String initMsg) {
                String[] inp = initMsg.split(",");
                //SubgraphValue value = new SubgraphValue();
                value.k = Integer.parseInt(inp[0]);
                value.maxEdgeCrossing = Long.parseLong(inp[1]);
                value.maxIterations = Integer.parseInt(inp[2]);
                // NOTE: Subgraph state is not available in the constructor.
                //getSubgraph().setSubgraphValue(value);

        }

        @Override
        public void compute(Iterable<IMessage<LongWritable, BytesWritable>> messageList) {

                ISubgraph<SubgraphValue, LongWritable, LongWritable, LongWritable, LongWritable, LongWritable> subgraph =
                    getSubgraph();
                // FIXME: Set the subgraph state in the first phase, since it is not allowed in the constructor.
                if (value.phase == Phase.PICK_LOCAL_CLUSTER)
                        subgraph.setSubgraphValue(value);
                SubgraphValue state = subgraph.getSubgraphValue();

                if (LOG.isInfoEnabled()) LOG.info("current phase : " + state.phase);
                if (LOG.isInfoEnabled()) LOG.info("Superstep " + getSuperstep() + " subgraph " + subgraph.getSubgraphId());


                if (state.phase == Phase.PICK_LOCAL_CLUSTER) {
                        init(state, subgraph.getVertices(), subgraph.getVertexCount());
                        List<Pair<Float, Long>> possibleCenters = localCenters(subgraph, state.k);
                        broadcastCenters(possibleCenters);
                        state.phase = Phase.PICK_GLOBAL_CLUSTER;
                } else if (state.phase == Phase.PICK_GLOBAL_CLUSTER) {
                        LOG.info("---------------- Subgraph "  + getSubgraph().getSubgraphId() + " --------");
                        PriorityQueue<Pair<Float, Long>> allCenters = accumulateAllCenters(messageList, state.k);
                        LOG.info("---------------- END "  + getSubgraph().getSubgraphId() + " --------");
                        List<Long> clusterCenters = kCenters(allCenters, getSubgraph(), state.k, state.dist);
                        Set<Long> remoteUpdated = localBfs(clusterCenters, getSubgraph(), state.dist);
                        packAndSendMessages(remoteUpdated, state.minSubgraphId, state.dist);
                        state.phase = Phase.CLUSTER_ASSIGNMENT;
                } else if (state.phase == Phase.CLUSTER_ASSIGNMENT) {
                        state.startNextPhase = (getSubgraph().getSubgraphId().get() == state.minSubgraphId);
                        // Local and remote vertices which have been updated
                        Set<Long> localUpdated = new HashSet<>();
                        state.startNextPhase =
                            readMessages(messageList, state.minSubgraphId, subgraph.getSubgraphId().get(), state.dist, localUpdated,
                                state.startNextPhase);
                        if (state.startNextPhase && (getSubgraph().getSubgraphId().get() == state.minSubgraphId)
                            && !state.clusterAssigned) {
                                // notifyPhaseChange
                                state.clusterAssigned = true;
                                sendToAll(new BytesWritable(new byte[] { 2 }));

                        } else if (state.startNextPhase) {
                                state.phase = Phase.EDGE_CROSSING;
                        } else {
                                Set<Long> remoteUpdated = localBfs(localUpdated, getSubgraph(), state.dist);
                                packAndSendMessages(remoteUpdated, state.minSubgraphId, state.dist);
                        }
                }

                else if (state.phase == Phase.EDGE_CROSSING) {
                        Map<Long, List<Pair<Long, Long>>> neighborClusters = new HashMap<>();
                        state.edgeCrossings = calculateEdgeCrossing(getSubgraph(), state.dist, neighborClusters);
                        sendClusterIds(neighborClusters);
                        state.phase = Phase.SEND_EDGE_CROSSING;
                } else if (state.phase == Phase.SEND_EDGE_CROSSING) {
                        long remoteEdgeCrossing = readClusterIds(messageList, state.dist);
                        state.edgeCrossings += remoteEdgeCrossing;
                        List<Pair<Float, Long>> possibleCenters = localCenters(getSubgraph(), state.k);
                        sendEdgeCrossing(state.edgeCrossings, possibleCenters);
                        state.phase = Phase.FINISHED;
                } else {
                        PriorityQueue<Pair<Float, Long>> allCenters =
                            sumEdgeCrossings(messageList, state.iterations, state.maxIterations, state.maxEdgeCrossing, state.k,
                                subgraph, state.dist);

                        if (allCenters == null)
                                voteToHalt();
                        else {
                                init(state, subgraph.getVertices(), subgraph.getVertexCount());
                                List<Long> clusterCenters = kCenters(allCenters, getSubgraph(), state.k, state.dist);
                                Set<Long> remoteUpdated = localBfs(clusterCenters, getSubgraph(), state.dist);
                                packAndSendMessages(remoteUpdated, state.minSubgraphId, state.dist);
                                state.phase = Phase.CLUSTER_ASSIGNMENT;
                                return;
                        }
                }

        }

        static void init(SubgraphValue stateUpdatable,
            Iterable<IVertex<LongWritable, LongWritable, LongWritable, LongWritable>> vertices, long vertexCount) {
                // FIXME: Later, when we have access to list of all subgraphs, we should use
                // that to pick the MIN subgraph as master
                //
                // Since there always exist a BSPPeer with ID 0 and within that one subgraph
                // with subgraph ID 0.
                stateUpdatable.minSubgraphId = 0;
                stateUpdatable.iterations++;
                stateUpdatable.dist = new HashMap<>((int) vertexCount);
                for (IVertex<LongWritable, LongWritable, LongWritable, LongWritable> v : vertices) {
                        stateUpdatable.dist.put(v.getVertexId().get(), new Pair<>(Integer.MAX_VALUE, -1l));
                }
                // clusterCenters = new ArrayList<Long>(k);
                stateUpdatable.clusterAssigned = false;
        }

        /*
         * Selects at most k local centers from the subgraph.
         * Based on the Full Scan algorithm from
         * https://eyalsch.wordpress.com/2010/04/01/random-sample/
         * Iterates through the list of local vertices once and selects each item with
         * a probability =
         * (no. of remaining to select)/(no. of remaining to scan). It guarantees that
         * exactly
         * k vertices are selected and each subset (of size = k) of the list is
         * equally likely.
         * In the case when k >= no. of local vertices, we choose all the vertices as
         * centers.
         */
        static List<Pair<Float, Long>> localCenters(
            ISubgraph<SubgraphValue, LongWritable, LongWritable, LongWritable, LongWritable, LongWritable> subgraph,
            int k) {
                long localVertexCount = subgraph.getLocalVertexCount();
                List<Pair<Float, Long>> possibleCenters = new ArrayList<>(k);
                Random randReservoir = new Random();

                if (k >= localVertexCount) {
                        for (IVertex<LongWritable, LongWritable, LongWritable, LongWritable> v : subgraph.getVertices()) {
                                if (!v.isRemote()) {
                                        if (LOG.isInfoEnabled())
                                                LOG.info("Local center: (K >= V) vertex " + v.getVertexId() + " subgraph " + subgraph.getSubgraphId());
                                        possibleCenters.add(new Pair<>(randReservoir.nextFloat(), v.getVertexId().get()));
                                }
                        }
                } else {
                        long itemsToSelect = k, visited = 0;
                        Iterator<IVertex<LongWritable, LongWritable, LongWritable, LongWritable>> it =
                            subgraph.getLocalVertices().iterator();

                        while (itemsToSelect > 0) {
                                IVertex<LongWritable, LongWritable, LongWritable, LongWritable> v = it.next();
                                if (randReservoir.nextFloat() < itemsToSelect / (localVertexCount - visited)) {
                                        if (LOG.isInfoEnabled())
                                                LOG.info("Local center: vertex " + v.getVertexId() + " subgraph " + subgraph.getSubgraphId());
                                        possibleCenters.add(new Pair<>(randReservoir.nextFloat(), v.getVertexId().get()));
                                        itemsToSelect--;
                                }
                                visited++;
                        }
                }

                return possibleCenters;
        }

        /*
         * Broadcasts the selected local centers along with their keys to every
         * partition.
         */
        void broadcastCenters(List<Pair<Float, Long>> possibleCenters) {
                Writer writer = new Writer((4 + 8) * possibleCenters.size());
                for (Pair<Float, Long> center : possibleCenters) {
                        writer.writeFloat(center.first);
                        writer.writeLong(center.second);
                        LOG.info("Sending " + center.first + "," + center.second);
                }
                BytesWritable message = new BytesWritable(writer.getBytes());
                sendToAll(message);
        }

        // Accumulates all the possible centers in one priority queue.
        static PriorityQueue<Pair<Float, Long>> accumulateAllCenters(
            Iterable<IMessage<LongWritable, BytesWritable>> messageList, int k) {
                // A rough estimate of no. of overall centers = 4 * k
                // FIXME: What is a tighter estimate?
                PriorityQueue<Pair<Float, Long>> allCenters = new PriorityQueue<>(4 * k, new PairComparator());
                for (IMessage<LongWritable, BytesWritable> messageItem : messageList) {
                        Reader reader = new Reader(messageItem.getMessage().copyBytes());
                        while (reader.available() > 0) {
                                float key = reader.readFloat();
                                long vertexId = reader.readLong();
                                LOG.info("Received " + key + "," + vertexId);
                                allCenters.add(new Pair<>(key, vertexId));
                        }
                }

                return allCenters;
        }

        /*
         * Selects k global centers deterministically from the received centers.
         * Based on Distributed Reservoir Sampling from
         * https://en.wikipedia.org/wiki/Reservoir_sampling
         *
         * Updates dist, returns 'k' cluster centers
         */
        static List<Long> kCenters(PriorityQueue<Pair<Float, Long>> allCentersUpdatable,
            ISubgraph<SubgraphValue, LongWritable, LongWritable, LongWritable, LongWritable, LongWritable> subgraph,
            int k, Map<Long, Pair<Integer, Long>> distUpdatable) {

                assert allCentersUpdatable.size() >= k;

                List<Long> clusterCenters = new ArrayList<>(k);
                while (clusterCenters.size() < k) {
                        Pair<Float, Long> center = allCentersUpdatable.poll();
                        if (LOG.isInfoEnabled()) LOG.info("Selecting " + center.second + " key " + center.first);
                        clusterCenters.add(center.second);
                        IVertex<LongWritable, LongWritable, LongWritable, LongWritable> v =
                            subgraph.getVertexById(new LongWritable(center.second));
                        if (v != null && !v.isRemote()) {
                                distUpdatable.put(center.second, new Pair<>(0, center.second));
                        }
                }

                return clusterCenters;
        }

        /**
         * Multi-source BFS.
         * Updates dist.
         *
         */
        static Set<Long> localBfs(Collection<Long> sourceVertices,
            ISubgraph<SubgraphValue, LongWritable, LongWritable, LongWritable, LongWritable, LongWritable> subgraph,
            Map<Long, Pair<Integer, Long>> distUpdatable) {

                // If we can get an estimate of the max queue size, we should set the
                // collection capacity to that value
                // NOTE: Since its a BFS and not SSSP, a priority queue is not required
                Queue<Long> queue = new ArrayDeque<>(sourceVertices.size());
                Set<Long> queueSet = new HashSet<>(sourceVertices.size());

                // Add valid source vertices to queue/set
                for (Long id : sourceVertices) {
                        IVertex<LongWritable, LongWritable, LongWritable, LongWritable> source =
                            subgraph.getVertexById(new LongWritable(id));
                        // ignore if it is a remote or does not exist in local subgraph
                        if (source == null || source.isRemote()) continue;
                        queue.add(id);
                        queueSet.add(id);
                }

                Set<Long> remoteUpdated = new HashSet<>();

                // get next item
                while (!queue.isEmpty()) {
                        long id = queue.remove();
                        queueSet.remove(id);

                        IVertex<LongWritable, LongWritable, LongWritable, LongWritable> source =
                            subgraph.getVertexById(new LongWritable(id));
                        Pair<Integer, Long> sourceDist = distUpdatable.get(id);

                        // i.e. we've only added visited vertices to queue
                        assert sourceDist.first != Integer.MAX_VALUE;

                        if (LOG.isInfoEnabled()) LOG.info("Starting BFS from " + id + " with cluster id " + sourceDist.second
                            + " in subgraph " + subgraph.getSubgraphId() + " having local vertices " + subgraph.getLocalVertexCount()
                            + " remote vertices " + (subgraph.getVertexCount() - subgraph.getLocalVertexCount()));

                        for (IEdge<LongWritable, LongWritable, LongWritable> edge : source.getOutEdges()) {
                                IVertex<LongWritable, LongWritable, LongWritable, LongWritable> adjVertex =
                                    subgraph.getVertexById(edge.getSinkVertexId());
                                Pair<Integer, Long> adjDist = distUpdatable.get(adjVertex.getVertexId().get());
                                if (adjDist.first > sourceDist.first + 1) { // is the distance improved?
                                        distUpdatable.put(adjVertex.getVertexId().get(), new Pair<>(sourceDist.first + 1, sourceDist.second));
                                        if (adjVertex.isRemote()) // cache for sending to remote
                                                remoteUpdated.add(adjVertex.getVertexId().get());
                                        else { // add to queue the local vertex if it is not already present
                                                if (!queueSet.contains(adjVertex.getVertexId().get())) {
                                                        queue.add(adjVertex.getVertexId().get());
                                                        queueSet.add(adjVertex.getVertexId().get());
                                                }
                                        }
                                }
                        }
                }
                return remoteUpdated;
        }

        /*
         * Packs the message in the form:
         * "targetVertexId minDistance assignedClusterId;" and sends them to
         * neighboring remote subgraphs.
         *
         * Also informs master (minSubgraphId) if more processing is required, or we
         * are done locally.
         */
        void packAndSendMessages(Set<Long> remoteUpdated, long minSubgraphId, Map<Long, Pair<Integer, Long>> dist) {
                if (remoteUpdated.isEmpty()) {
                        sendMessage(new LongWritable(minSubgraphId), new BytesWritable(new byte[] { 0 }));
                        return;
                } else {
                        sendMessage(new LongWritable(minSubgraphId), new BytesWritable(new byte[] { 1 }));
                }

                Map<Long, Writer> msg = new HashMap<>();
                for (Long remoteVertexId : remoteUpdated) {
                        Pair<Integer, Long> remoteDist = dist.get(remoteVertexId);
                        IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex =
                            getSubgraph().getVertexById(new LongWritable(remoteVertexId));

                        assert vertex.isRemote();
                        @SuppressWarnings("unchecked")
                        IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable> remoteVertex =
                            (IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable>) vertex;

                        Writer writer = msg.get(remoteVertex.getSubgraphId().get());
                        if (writer == null) {
                                writer = new Writer();
                                msg.put(remoteVertex.getSubgraphId().get(), writer);
                        }
                        writer.writeLong(remoteVertexId);
                        writer.writeInt(remoteDist.first);
                        writer.writeLong(remoteDist.second);
                }
                for (Map.Entry<Long, Writer> entry : msg.entrySet()) {
                        sendMessage(new LongWritable(entry.getKey()), new BytesWritable(entry.getValue().getBytes()));
                }
        }

        /*
         * Unpacks the messages and updates the distance if it is better than the
         * current distance.
         */
        static boolean readMessages(Iterable<IMessage<LongWritable, BytesWritable>> messageList, long minSubgraphId,
            long subgraphId, Map<Long, Pair<Integer, Long>> dist, Set<Long> localVerticesUpdatable,
            boolean startNextPhasePrev) {

                boolean startNextPhase = startNextPhasePrev;
                // Set<Long> localUpdated = new HashSet<>();Set<Long> localUpdated = new
                // HashSet<>();
                for (IMessage<LongWritable, BytesWritable> messageItem : messageList) {
                        BytesWritable msg = messageItem.getMessage();
                        Reader reader = new Reader(msg.copyBytes());

                        if (reader.available() == 1) {
                                byte b = reader.readByte();
                                // x += msg;
                                if (subgraphId == minSubgraphId) {
                                        if (b == 1) startNextPhase = false; // BFS is still going on
                                }
                                if (b == 2) startNextPhase = true;
                                continue;
                        }

                        while (reader.available() > 0) {
                                Long targetId = reader.readLong();
                                Integer minDist = reader.readInt();
                                Long clusterId = reader.readLong();
                                Pair<Integer, Long> prevDist = dist.get(targetId);
                                if (minDist < prevDist.first) {
                                        dist.put(targetId, new Pair<>(minDist, clusterId));
                                        localVerticesUpdatable.add(targetId);
                                }
                        }
                }
                return startNextPhase;
        }


        // Edge crossing is counted for each edge, so it will be double in case of
        // undirected graphs.
        // return edge cuts, and populates neighborClusters
        static long calculateEdgeCrossing(
            ISubgraph<SubgraphValue, LongWritable, LongWritable, LongWritable, LongWritable, LongWritable> subgraph,
            Map<Long, Pair<Integer, Long>> dist, Map<Long, List<Pair<Long, Long>>> neighborClustersUpdatable) {

                long edgeCrossing = 0;

                for (IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex : subgraph.getLocalVertices()) {
                        long clusterId = dist.get(vertex.getVertexId().get()).second;
                        for (IEdge<LongWritable, LongWritable, LongWritable> edge : vertex.getOutEdges()) {
                                IVertex<LongWritable, LongWritable, LongWritable, LongWritable> adjVertex =
                                    subgraph.getVertexById(edge.getSinkVertexId());

                                // if sink is remote, then notify of this vertex's cluster ID
                                if (adjVertex.isRemote()) {
                                        @SuppressWarnings("unchecked")
                                        long remoteSubgaphId =
                                            ((IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable>) adjVertex)
                                                .getSubgraphId().get();

                                        List<Pair<Long, Long>> localClusterIds = neighborClustersUpdatable.get(remoteSubgaphId);
                                        if (localClusterIds == null) {
                                                localClusterIds = new ArrayList<>();
                                                neighborClustersUpdatable.put(remoteSubgaphId, localClusterIds);
                                        }
                                        // send the source vertex's cluster ID to the sink vertex
                                        localClusterIds.add(new Pair<>(adjVertex.getVertexId().get(), clusterId));

                                } else // !adjVertex.isRemote() and...
                                        if (clusterId != dist.get(adjVertex.getVertexId().get()).second) {
                                                edgeCrossing++; // Will double count in case of undirected graph.
                                        }
                        }
                }
                return edgeCrossing;
        }

        void sendClusterIds(Map<Long, List<Pair<Long, Long>>> neighborClusters) {
                for (Map.Entry<Long, List<Pair<Long, Long>>> e : neighborClusters.entrySet()) {
                        List<Pair<Long, Long>> msgList = e.getValue();
                        Writer writer = new Writer(msgList.size() * 2 * 8);
                        for (Pair<Long, Long> msg : msgList) {
                                writer.writeLong(msg.first);
                                writer.writeLong(msg.second);
                        }
                        sendMessage(new LongWritable(e.getKey()), new BytesWritable(writer.getBytes()));
                }
        }

        static long readClusterIds(Iterable<IMessage<LongWritable, BytesWritable>> messageList,
            Map<Long, Pair<Integer, Long>> dist) {
                long edgeCrossing = 0;
                for (IMessage<LongWritable, BytesWritable> messageItem : messageList) {
                        Reader reader = new Reader(messageItem.getMessage().copyBytes());
                        while (reader.available() > 0) {
                                long targetId = reader.readLong();
                                long neighborClusterId = reader.readLong();
                                long clusterId = dist.get(targetId).second;
                                if (clusterId != neighborClusterId) edgeCrossing++;
                        }
                }

                return edgeCrossing;
        }

        void sendEdgeCrossing(long edgeCuts, List<Pair<Float, Long>> possibleCenters) {
                Writer writer = new Writer(8 + (4 + 8) * possibleCenters.size());
                writer.writeLong(edgeCuts);

                for (Pair<Float, Long> center : possibleCenters) {
                        writer.writeFloat(center.first);
                        writer.writeLong(center.second);
                }
                BytesWritable message = new BytesWritable(writer.getBytes());
                sendToAll(message);
        }

        static PriorityQueue<Pair<Float, Long>> sumEdgeCrossings(Iterable<IMessage<LongWritable, BytesWritable>> messageList,
            int iterations, int maxIterations, long maxEdgeCrossing, int k,
            ISubgraph<SubgraphValue, LongWritable, LongWritable, LongWritable, LongWritable, LongWritable> subgraph,
            Map<Long, Pair<Integer, Long>> dist) {

                long edgeCrossing = 0; // Avoid over-counting own edge crossing count.
                List<Reader> messageReaders = new LinkedList<>(); // keep track of readers
                for (IMessage<LongWritable, BytesWritable> messageItem : messageList) {
                        Reader reader = new Reader(messageItem.getMessage().copyBytes());
                        messageReaders.add(reader);
                        edgeCrossing += reader.readLong();
                }

                if (LOG.isInfoEnabled()) LOG.info("Edge crossing " + edgeCrossing);

                if (LOG.isInfoEnabled()) LOG.info("Checking termination");

                if (edgeCrossing <= maxEdgeCrossing || iterations >= maxIterations) {
                        if (LOG.isInfoEnabled()) LOG.info("Termination possible. Edge cuts=" + edgeCrossing + "; Iters=" + iterations);
                        // All done.
                        for (Map.Entry<Long, Pair<Integer, Long>> e : dist.entrySet()) {
                                if (!(subgraph.getVertexById(new LongWritable(e.getKey())).isRemote()))
                                        System.out.println(e.getKey() + " " + e.getValue().second);
                        }
                        return null; // nothing more to do
                } else {

                        PriorityQueue<Pair<Float, Long>> allCenters = new PriorityQueue<>(4 * k, new PairComparator());
                        if (LOG.isInfoEnabled()) LOG.info("Termination not yet possible");
                        // process rest of message payload

                        for (Reader reader : messageReaders) {
                                while (reader.available() > 0) {
                                        float key = reader.readFloat();
                                        long vertexId = reader.readLong();
                                        allCenters.add(new Pair<>(key, vertexId));
                                }
                        }
                        return allCenters;
                }
        }
}
