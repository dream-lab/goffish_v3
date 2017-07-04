package in.dream_lab.goffish.sample;

import in.dream_lab.goffish.api.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * Returns an edge list of a Spanning Forest of the given graph.
 * In the first superstep, it does a local BFS on every subgraph
 * to find a spanning tree within every subgraph. In the consecutive
 * supersteps, it forms a spanning forest on the meta-graph.
 *
 *
 *
 *
 * @author Hullas Jindal
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

public class SpanningForest extends
        AbstractSubgraphComputation<LongWritable, LongWritable, LongWritable, MultipleLongWritable, LongWritable, LongWritable, LongWritable>
        implements ISubgraphWrapup{

    long cid;
    MultipleLongWritable edge=null;
    Map<LongWritable, MultipleLongWritable> messagePair = new HashMap<>();
    Map<LongWritable, Boolean> visited = new HashMap<>();
    Queue<IVertex<LongWritable, LongWritable, LongWritable, LongWritable>> queue = new ArrayDeque<>();

    @Override
    public void compute(Iterable<IMessage<LongWritable, MultipleLongWritable>> messages) throws IOException {
        if(getSuperstep()==0){
            cid=getSubgraph().getSubgraphId().get();
            for(IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex : getSubgraph().getLocalVertices()){
                visited.put(vertex.getVertexId(), false);
            }
            IVertex<LongWritable, LongWritable, LongWritable, LongWritable> initVertex = getSubgraph().getLocalVertices().iterator().next();
            visited.put(initVertex.getVertexId(), true);
            queue.add(initVertex);
            BFS();

            sendMessage();
        }
        else{
            boolean changed = false;
            for(IMessage<LongWritable, MultipleLongWritable> message : messages){
                if(message.getMessage().getId3()<cid){
                    cid=message.getMessage().getId3();
                    edge = message.getMessage();
                    changed = true;
                }
            }
            if(changed){
                sendMessage();
            }
        }
        voteToHalt();
    }

    public void BFS(){
        while(!queue.isEmpty()) {
            IVertex<LongWritable, LongWritable, LongWritable, LongWritable> source = queue.poll();
            for (IEdge<LongWritable, LongWritable, LongWritable> edge : source.getOutEdges()) {
                IVertex<LongWritable, LongWritable, LongWritable, LongWritable> neighbour = getSubgraph().getVertexById(edge.getSinkVertexId());
                if (!neighbour.isRemote()) {
                    if (!visited.get(neighbour.getVertexId())) {
                        System.out.println(source.getVertexId() + " - " + neighbour.getVertexId());
                        visited.put(neighbour.getVertexId(), true);
                        queue.add(neighbour);
                    }
                } else {
                    messagePair.put(((IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable>)
                                    neighbour).getSubgraphId(),
                            new MultipleLongWritable(source.getVertexId().get(), neighbour.getVertexId().get()));
                }
            }
        }
    }

    public void sendMessage(){
        Iterator it = messagePair.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<LongWritable, MultipleLongWritable> pair = (Map.Entry) it.next();
            sendMessage(pair.getKey(), new MultipleLongWritable(pair.getValue().getId1(), pair.getValue().getId2(), cid));
        }
    }

    @Override
    public void wrapup() throws IOException {
        if(cid!=getSubgraph().getSubgraphId().get())
            System.out.println(edge.getId1() + " - " + edge.getId2());
    }

}

class MultipleLongWritable implements Writable {

    public long id1;
    public long id2;
    public long id3;

    public MultipleLongWritable() {
    }

    public MultipleLongWritable(long id1) {
        this.id1 = id1;
    }

    public MultipleLongWritable(long id1, long id2) {
        this.id1 = id1;
        this.id2 = id2;
    }

    public MultipleLongWritable(long id1, long id2, long id3) {
        this.id1 = id1;
        this.id2 = id2;
        this.id3 = id3;
    }

    public long getId1() {
        return id1;
    }

    public long getId2() {
        return id2;
    }

    public long getId3() {
        return id3;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(id1);
        dataOutput.writeLong(id2);
        dataOutput.writeLong(id3);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id1 = dataInput.readLong();
        id2 = dataInput.readLong();
        id3 = dataInput.readLong();
    }
}

