package in.dream_lab.goffish.sample;

import in.dream_lab.goffish.api.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * Returns core number for each vertex.
 * Reduces the number of super-steps taken, when compared to KCoreFast by
 * processing the vertices in order of estimated core values, but consumes
 * more memory.
 *
 * @author Tilak S Naik
 * @author Yogesh Simmhan
 * @version 1.0
 * @see <a href="http://www.dream-lab.in/">DREAM:Lab</a>
 * <p>
 * Copyright 2014 DREAM:Lab, Indian Institute of Science, Bangalore
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class KCoreSort extends AbstractSubgraphComputation<LongWritable, LongWritable, LongWritable, KCoreSortMessage, LongWritable, LongWritable, LongWritable> implements ISubgraphWrapup {

    private Map<Long, Integer> degrees = new HashMap<>(), cores = new HashMap<>();
    private Map<Long, Set<Long>> subGraphIds = new HashMap<>(), remoteNeighbors = new HashMap<>();
    private Map<Integer, Set<Long>> coreSets = new HashMap<>();

    @Override
    public void compute(Iterable<IMessage<LongWritable, KCoreSortMessage>> iMessages) throws IOException {
        boolean process = getSuperstep() == 0;
        if (process)
            initializeSubGraph();
        else
            for (IMessage<LongWritable, KCoreSortMessage> iMessage: iMessages)
                process = handleMessage(iMessage.getMessage()) || process;
        if (process)
            localEstimate();
        voteToHalt();
    }

    private void localEstimate() {
        Map<Long, Integer> coreNumbers = new HashMap<>(degrees);
        Map<Integer, Set<Long>> coreSet = new TreeMap<>();
        for (int core: coreSets.keySet())
            coreSet.put(core, new HashSet<>(coreSets.get(core)));
        while (coreSet.size() > 0) {
            int core = coreSet.keySet().iterator().next();
            for (long vertexId: coreSet.remove(core)) {
                if (isRemote(vertexId)) {
                    for (long neighborId: remoteNeighbors.get(vertexId)) {
                        int oldCore = coreNumbers.get(neighborId);
                        if (oldCore > core) {
                            coreSet.get(oldCore--).remove(neighborId);
                            coreNumbers.put(neighborId, oldCore);
                            Set<Long> set = coreSet.get(oldCore);
                            if (set == null) {
                                set = new HashSet<>();
                                set.add(neighborId);
                                coreSet.put(oldCore, set);
                            }
                            else
                                set.add(neighborId);
                        }
                    }
                }
                else {
                    if (cores.get(vertexId) > core) {
                        cores.put(vertexId, core);
                        informNeighbors(vertexId, core);
                    }
                    for (IEdge<LongWritable, LongWritable, LongWritable> edge: getSubgraph().getVertexById(new LongWritable(vertexId)).getOutEdges()) {
                        long neighborId = edge.getSinkVertexId().get();
                        if (!isRemote(neighborId)) {
                            int oldCore = coreNumbers.get(neighborId);
                            if (oldCore > core) {
                                coreSet.get(oldCore--).remove(neighborId);
                                coreNumbers.put(neighborId, oldCore);
                                Set<Long> set = coreSet.get(oldCore);
                                if (set == null) {
                                    set = new HashSet<>();
                                    set.add(neighborId);
                                    coreSet.put(oldCore, set);
                                }
                                else
                                    set.add(neighborId);
                            }
                        }
                    }
                }
            }
        }
    }

    private boolean isRemote(long vertexId) {
        return degrees.get(vertexId) == null;
    }

    private boolean handleMessage(KCoreSortMessage message) {
        long vertexId = message.getVertexId();
        int core = message.getCore();
        int oldCore = cores.get(vertexId);
        if (oldCore <= core)
            return false;
        cores.put(vertexId, core);
        Set<Long> coreSet = coreSets.get(oldCore);
        coreSet.remove(vertexId);
        if (coreSet.size() == 0)
            coreSets.remove(oldCore);
        if (coreSets.get(core) == null)
            coreSets.put(core, new HashSet<Long>());
        coreSets.get(core).add(vertexId);
        return true;
    }

    private void informNeighbors(long vertexId, int core) {
        KCoreSortMessage message = new KCoreSortMessage(vertexId, core);
        for (long subGraphId: subGraphIds.get(vertexId))
            sendMessage(new LongWritable(subGraphId), message);
    }

    private void initializeSubGraph() {
        Set<Long> remoteCoreSet = new HashSet<>();
        for (IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable> remoteVertex: getSubgraph().getRemoteVertices()) {
            long remoteVertexId = remoteVertex.getVertexId().get();
            remoteNeighbors.put(remoteVertexId, new HashSet<Long>());
            cores.put(remoteVertexId, Integer.MAX_VALUE);
            remoteCoreSet.add(remoteVertexId);
        }
        coreSets.put(Integer.MAX_VALUE, remoteCoreSet);
        for (IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex: getSubgraph().getLocalVertices())
            initializeVertex(vertex);
    }

    private void initializeVertex(IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex) {
        long vertexId = vertex.getVertexId().get();
        Collection<IEdge<LongWritable, LongWritable, LongWritable>> edges = (Collection<IEdge<LongWritable,LongWritable,LongWritable>>) vertex.getOutEdges();
        int degree = edges.size();
        degrees.put(vertexId, degree);
        cores.put(vertexId, degree + 1);
        if (coreSets.get(degree) == null)
            coreSets.put(degree, new HashSet<Long>());
        coreSets.get(degree).add(vertexId);
        Set<Long> subGraphIdSet = new HashSet<>();
        for (IEdge<LongWritable, LongWritable, LongWritable> edge: edges) {
            IVertex<LongWritable, LongWritable, LongWritable, LongWritable> neighbor = getSubgraph().getVertexById(edge.getSinkVertexId());
            if (neighbor.isRemote()) {
                subGraphIdSet.add(((IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable>) neighbor).getSubgraphId().get());
                remoteNeighbors.get(edge.getSinkVertexId().get()).add(vertexId);
            }
        }
        subGraphIds.put(vertexId, subGraphIdSet);
    }

    @Override
    public void wrapup() throws IOException {
        for (long vertexId: degrees.keySet())
            System.out.println(vertexId + " " + cores.get(vertexId));
    }
}

class KCoreSortMessage implements Writable {
    private long vertexId;
    private int core;

    public KCoreSortMessage() {}

    public KCoreSortMessage(long vertexId, int core) {
        this.vertexId = vertexId;
        this.core = core;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(vertexId);
        dataOutput.writeInt(core);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        vertexId = dataInput.readLong();
        core = dataInput.readInt();
    }

    public long getVertexId() {
        return vertexId;
    }

    public int getCore() {
        return core;
    }
}