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

package in.dream_lab.goffish.sample;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;

import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IMessage;
import in.dream_lab.goffish.api.IRemoteVertex;
import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.sample.ByteArrayHelper.Reader;
import in.dream_lab.goffish.sample.ByteArrayHelper.Writer;
import in.dream_lab.goffish.api.AbstractSubgraphComputation;


/*
 * Ported from goffish v2
 */

/**
 * Counts and lists all the triangles found in an undirected graph. A triangle
 * can be classified into three types based on the location of its vertices: i)
 * All the vertices lie in the same partition, ii) two of the vertices lie in
 * the same partition and iii) all the vertices lie in different partitions. (i)
 * and (ii) types of triangle can be identified with the information available
 * within the subgraph. For (iii) type of triangles three supersteps are
 * required.
 *
 * @author Diptanshu Kakwani
 * @author Yogesh Simmhan
 *
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

public class TriangleCount extends
        AbstractSubgraphComputation<LongWritable, LongWritable, LongWritable, BytesWritable, LongWritable, LongWritable, LongWritable> {

    private long triangleCount; // State is persisted in subgraph

    // FIXME: State must be persisted in subgraph, if list of vertex triples for
    // triangles is required
    // private List<long[]> trianglesList = new LinkedList<>();
    private List<long[]> trianglesList = null;

    public static final Log LOG = LogFactory.getLog(TriangleCount.class);

    @Override
    public void compute(Iterable<IMessage<LongWritable, BytesWritable>> messageList) throws IOException {
        if (getSuperstep() == 0) {
            // Maintains byte array for messages destined to each subgraph.
            // Each message has a pair of long values, for the first VID in this
            // subgraph and
            // the second/target VID in the destination subgraph.
            Map<Long, ByteArrayHelper.Writer> outputMessages = new HashMap<Long, ByteArrayHelper.Writer>();

            // iterate through all vertices, and each of their out edges to determine
            // the type of triangle
            for (IVertex<LongWritable, LongWritable, LongWritable, LongWritable> firstVertex : getSubgraph()
                    .getLocalVertices()) {
                for (IEdge<LongWritable, LongWritable, LongWritable> edge : firstVertex.getOutEdges()) {
                    IVertex<LongWritable, LongWritable, LongWritable, LongWritable> secondVertex =
                            getSubgraph().getVertexById(edge.getSinkVertexId());

                    // 1) If (adj vertex has a lower value) do nothing
                    if (firstVertex.getVertexId().get() > secondVertex.getVertexId().get()) continue;

                    // TYPE 3 triangle: 1 local 2 remote and in different subgraphs
                    // 2) If adjacent vertex is remote AND has a higher value, pass
                    // <secondVID,firstVID>, where firstVID is this vertex ID and
                    // secondVID is adjVertex (target vertex)
                    if (secondVertex.isRemote()) {
                        @SuppressWarnings("unchecked")
                        long secondSGId =
                                ((IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable>) secondVertex)
                                        .getSubgraphId().get();
                        addStep1Message(outputMessages, secondSGId, secondVertex.getVertexId().get(),
                                firstVertex.getVertexId().get());
                        // Done with (remote) second vertex.
                        continue;
                    }

                    // TYPE 1 or 2 triangle
                    // 3) If adjacent vertex local AND has a higher value (Type 1), or
                    // adjacent vertex is remote (Type 2)
                    // Counting triangles which have at least two vertices in the same
                    // subgraph.
                    for (IEdge<LongWritable, LongWritable, LongWritable> secondEdge : secondVertex.getOutEdges()) {
                        IVertex<LongWritable, LongWritable, LongWritable, LongWritable> thirdVertex =
                                getSubgraph().getVertexById(secondEdge.getSinkVertexId());
                        if (thirdVertex.isRemote() || // TYPE 2: 2 local 1 remote
                                // TYPE 1: all 3 local
                                thirdVertex.getVertexId().get() > secondVertex.getVertexId().get()) {
                            // check if first is connected to third
                            if (firstVertex.getOutEdge(thirdVertex.getVertexId()) != null) {
                                triangleCount++;
                                if (trianglesList != null) trianglesList.add(new long[]{firstVertex.getVertexId().get(),
                                        secondVertex.getVertexId().get(), thirdVertex.getVertexId().get()});
                            }
                        }
                    }
                } // done with all edges for first vertex
            } // done with first vertex

            sendMessages(outputMessages);

        } else if (getSuperstep() == 1) {
            // process <secondVID,firstVID> messages where second VID is the target
            // vertex in this subgraph
            Map<Long, List<Long>> secondToFirstIds = new HashMap<Long, List<Long>>();
            Map<Long, Writer> outputMessages = new HashMap<Long, Writer>();
            unpackStep1Messages(messageList, secondToFirstIds);

            // iterate over <secondVID, <firstVID1,firstVID2,...>>
            for (Map.Entry<Long, List<Long>> secondToFirst : secondToFirstIds.entrySet()) {

                // this vertex is the second vertex
                IVertex<LongWritable, LongWritable, LongWritable, LongWritable> secondVertex =
                        getSubgraph().getVertexById(new LongWritable(secondToFirst.getKey()));
                // list of first VIDs for this second vertex received from sstep 1
                List<Long> firstList = secondToFirst.getValue();

                // TYPE 3
                // If adjacent (third) vertex is remote AND has a higher value than this
                // (second) vertex AND is not in same SG as first,
                // pass <thirdVID,secondVID,firstVID1,firstVID2,...>
                for (IEdge<LongWritable, LongWritable, LongWritable> secondEdge : secondVertex.getOutEdges()) {
                    IVertex<LongWritable, LongWritable, LongWritable, LongWritable> thirdVertex =
                            getSubgraph().getVertexById(secondEdge.getSinkVertexId());

                    if (thirdVertex.isRemote() && thirdVertex.getVertexId().get() > secondVertex.getVertexId().get()) {
                        @SuppressWarnings("unchecked")
                        long thirdSGId =
                                ((IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable>) thirdVertex)
                                        .getSubgraphId().get();

                        List<Long> firstSublist = new ArrayList<>(firstList.size());
                        for (Long first : firstList) {
                            LongWritable firstId = new LongWritable(first);
                            @SuppressWarnings("unchecked")
                            IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable> firstVertex =
                                    (IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable>) getSubgraph()
                                            .getVertexById(firstId);
                            // since we received a message from first, there should be an edge
                            // from second to first as well
                            assert firstVertex != null;
                            if (firstVertex.getSubgraphId().get() != thirdSGId) firstSublist.add(first);
                        }
                        // Later, think about using Java 8 stream:
                        // firstList.stream().filter(l -> (((IRemoteVertex<LongWritable,
                        // LongWritable, LongWritable, LongWritable, LongWritable>)
                        // getSubgraph().getVertexById(new
                        // LongWritable(l))).getSubgraphId().get() != thirdSGId));

                        // Add message for <thirdVID,secondVID,firstVID1,firstVID2,...>,
                        // if first not in same SG as third
                        if (!firstSublist.isEmpty())
                            addStep2Message(outputMessages, thirdSGId, thirdVertex.getVertexId().get(),
                                    secondVertex.getVertexId().get(), firstSublist);
                    }
                }
            }
            sendMessages(outputMessages);

        } else {
            // use this if we do not need to maintain/print the actual triangle
            // triples
            if (trianglesList == null) {
                // Returns a map from third vertex to the list of first vertices for it,
                // along with the count of number of second vertices through which each
                // first vertex can reach the third vertex. This can reduce the number
                // of
                // iterations/lookups.
                Map<Long, Map<Long, Integer>> thirdToFirstIds = new HashMap<Long, Map<Long, Integer>>();
                unpackStep2MessageSet(messageList, thirdToFirstIds);

                // TODO: Check if returning a list of non-distinct first vertex IDs
                // without count is faster

                for (Entry<Long, Map<Long, Integer>> entry : thirdToFirstIds.entrySet()) {
                    IVertex<LongWritable, LongWritable, LongWritable, LongWritable> thirdVertex =
                            getSubgraph().getVertexById(new LongWritable(entry.getKey()));
                    for (Entry<Long, Integer> firstAndCount : entry.getValue().entrySet()) {
                        if (thirdVertex.getOutEdge(new LongWritable(firstAndCount.getKey())) != null) {
                            // increment count by number of second vertices for this same
                            // first vertex
                            triangleCount += firstAndCount.getValue();
                        }
                    }
                }

            } else {

                // Returns a map from the third vertex to list of <first,second> vertex
                // pairs present in all messages.
                Map<Long, List<Pair<Long, Long>>> thirdToFirstAndSecondIds = new HashMap<Long, List<Pair<Long, Long>>>();
                unpackStep2MessageList(messageList, thirdToFirstAndSecondIds);

                for (Map.Entry<Long, List<Pair<Long, Long>>> entry : thirdToFirstAndSecondIds.entrySet()) {
                    IVertex<LongWritable, LongWritable, LongWritable, LongWritable> thirdVertex =
                            getSubgraph().getVertexById(new LongWritable(entry.getKey()));
                    for (Pair<Long, Long> firstAndSecond : entry.getValue()) {
                        if (thirdVertex.getOutEdge(new LongWritable(firstAndSecond.left)) != null) {
                            triangleCount++;
                            trianglesList
                                    .add(new long[]{firstAndSecond.left, firstAndSecond.right, thirdVertex.getVertexId().get()});
                        }
                    }
                }
            }
        }

        // Update the state of the subgraph
        getSubgraph().setSubgraphValue(new LongWritable(triangleCount));
        // FIXME: if trianglesList != null, add it to subgraph state too
        if (trianglesList != null) {
            for (long[] triangle : trianglesList) {
                LOG.info(Arrays.toString(triangle));
            }
        }

        voteToHalt();
    }

    private class Pair<L, R> {
        private L left;
        private R right;

        Pair(L l, R r) {
            left = l;
            right = r;
        }

        L getleft() {
            return left;
        }

        R getRight() {
            return right;
        }
    }

    private void addStep1Message(Map<Long, ByteArrayHelper.Writer> msg, long remoteSubgraphId, long secondVID,
                                 long firstVID) {
        Writer writer = msg.get(remoteSubgraphId);
        if (writer == null) {
            writer = new Writer();
            msg.put(remoteSubgraphId, writer);
        }
        writer.writeLong(secondVID);
        writer.writeLong(firstVID);
    }

    void unpackStep1Messages(Iterable<IMessage<LongWritable, BytesWritable>> messageList,
                             Map<Long, List<Long>> secondToFirstIds) {
        for (IMessage<LongWritable, BytesWritable> messageItem : messageList) {
            BytesWritable message = messageItem.getMessage();
            ByteArrayHelper.Reader reader = new Reader(message.copyBytes());
            while (reader.available() >= 16) {
                long second = reader.readLong();
                long first = reader.readLong();
                List<Long> firstList = secondToFirstIds.get(second);
                if (firstList == null) {
                    firstList = new LinkedList<Long>();
                    secondToFirstIds.put(second, firstList);
                }
                firstList.add(first);
            }

            if (reader.available() > 0)
                throw new RuntimeException("reader is not empty but has less than 16 bytes. " + reader.available());
        }
    }

    private void addStep2Message(Map<Long, ByteArrayHelper.Writer> msg, long thirdSGId, long thirdVID, long secondVID,
                                 List<Long> firstList) {
        Writer writer = msg.get(thirdSGId);
        if (writer == null) {
            writer = new Writer();
            msg.put(thirdSGId, writer);
        }
        writer.writeLong(thirdVID);
        writer.writeLong(secondVID);
        writer.writeManyLongs(firstList);
        //LOG.info("LIST SIZE: " + firstList.size());
    }

    void unpackStep2MessageSet(Iterable<IMessage<LongWritable, BytesWritable>> messageList,
                               Map<Long, Map<Long, Integer>> thirdToFirstIds) {
        for (IMessage<LongWritable, BytesWritable> messageItem : messageList) {
            BytesWritable message = messageItem.getMessage();
            ByteArrayHelper.Reader reader = new Reader(message.copyBytes());

            // Maintain a map from third to set of distinct first vertices along with
            // their counts
            while (reader.available() >= (8 + 8 + 4 + 8)) { // third,second,firstCount,first+
                long third = reader.readLong();
                long second = reader.readLong();
                List<Long> firstIds = reader.readLongList();
                Map<Long, Integer> firstAndCountMap = thirdToFirstIds.get(third);
                if (firstAndCountMap == null) {
                    firstAndCountMap = new HashMap<Long, Integer>((int) (firstIds.size() / 0.75));
                    thirdToFirstIds.put(third, firstAndCountMap);
                }

                // add/update entry for first vertex corresponding to the third vertex
                for (Long first : firstIds) {
                    Integer count = firstAndCountMap.get(first);
                    if (count == null)
                        firstAndCountMap.put(first, 1);
                    else
                        firstAndCountMap.put(first, count + 1);
                }
            }

            if (reader.available() > 0) throw new RuntimeException(
                    "reader is not empty but has less than " + (8 + 8 + 4 + 8) + " bytes. " + reader.available());
        }
    }

    void unpackStep2MessageList(Iterable<IMessage<LongWritable, BytesWritable>> messageList,
                                Map<Long, List<Pair<Long, Long>>> thirdToFirstAndSecondIds) {
        for (IMessage<LongWritable, BytesWritable> messageItem : messageList) {
            BytesWritable message = messageItem.getMessage();
            ByteArrayHelper.Reader reader = new Reader(message.copyBytes());
            // Maintain a map from third to list of <first,second> vertex pairs
            while (reader.available() >= (8 + 8 + 4 + 8)) { // third,second,firstCount,first+
                long third = reader.readLong();
                long second = reader.readLong();
                List<Long> firstIds = reader.readLongList();
                List<Pair<Long, Long>> firstAndSecondList = thirdToFirstAndSecondIds.get(third);
                if (firstAndSecondList == null) {
                    firstAndSecondList = new LinkedList<Pair<Long, Long>>();
                    thirdToFirstAndSecondIds.put(third, firstAndSecondList);
                }

                for (Long first : firstIds) {
                    firstAndSecondList.add(new Pair<Long, Long>(first, second));
                }
            }

            if (reader.available() > 0) throw new RuntimeException(
                    "reader is not empty but has less than " + (8 + 8 + 4 + 8) + " bytes. " + reader.available());
        }
    }

    void sendMessages(Map<Long, ByteArrayHelper.Writer> msg) {
        for (Map.Entry<Long, ByteArrayHelper.Writer> m : msg.entrySet()) {
            BytesWritable message = new BytesWritable(m.getValue().getBytes());
            sendMessage(new LongWritable(m.getKey()), message);
        }
    }

}
