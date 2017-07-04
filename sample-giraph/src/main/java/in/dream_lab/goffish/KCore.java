package in.dream_lab.goffish;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Ensure "-vof in.dream_lab.goffish.MyOutputWriter"
 *
 * Returns core number for each vertex ID
 * Reference:
 *      A. Montresor, F. D. Pellegrini, and D. Miorandi, “Distributed
        k-core decomposition,” IEEE Transactions on Parallel and
        Distributed Systems, vol. 24, no. 2, pp. 288–300, 2013
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

public class KCore extends BasicComputation<IntWritable, MyIntMapWritable, NullWritable, MyIntIntWritable>{

    boolean changed;
    Map<IntWritable, IntWritable> core = new HashMap<IntWritable, IntWritable>();

    @Override
    public void compute(Vertex<IntWritable, MyIntMapWritable, NullWritable> vertex, Iterable<MyIntIntWritable> messages)
            throws IOException {
        if (getSuperstep()==0){
            changed=false;
            for(Edge<IntWritable, NullWritable> edge : vertex.getEdges())
                core.put(edge.getTargetVertexId(), new IntWritable(Integer.MAX_VALUE));
            vertex.setValue(new MyIntMapWritable(vertex.getNumEdges(), core));
            changed=true;
        }
        else{
            changed=false;
            core=vertex.getValue().getMAP();
            for(MyIntIntWritable message : messages)
                core.put(new IntWritable(message.getId1()), new IntWritable(message.getId2()));
            int k=vertex.getValue().getINT(), pos, count[] = new int[k+1];
            for(int i=0;++i<=k;count[i]=0);
            for(Edge<IntWritable, NullWritable> edge : vertex.getEdges())
                count[Math.min(k, core.get(edge.getTargetVertexId()).get())]++;
            for(int i=k;i>=2;i--)
                count[i-1]+=count[i];
            for(pos=k;pos>1&&count[pos]<pos;pos--);
            if(pos<k){
                vertex.setValue(new MyIntMapWritable(pos, core));
                changed=true;
            }
            else
                vertex.setValue(new MyIntMapWritable(k, core));
        }
        if(changed)
            sendMessageToAllEdges(vertex, new MyIntIntWritable(vertex.getId().get(), vertex.getValue().getINT()));
        vertex.voteToHalt();
    }
}