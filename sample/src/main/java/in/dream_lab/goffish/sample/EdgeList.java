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

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;

import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IMessage;
import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.api.AbstractSubgraphComputation;

/*
 * Prints the edge list
 * prints for only the vertices belonging to that partition 
 */

public class EdgeList extends
    AbstractSubgraphComputation<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable, LongWritable, LongWritable> {

  @Override
  public void compute(Iterable<IMessage<LongWritable, LongWritable>> messages)
      throws IOException {
    if (getSuperstep() == 0) {
      for (IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex : getSubgraph()
          .getLocalVertices()) {
        for (IEdge<LongWritable, LongWritable, LongWritable> e : vertex
            .getOutEdges()) {
          System.out.println(vertex.getVertexId() + "\t" + e.getSinkVertexId());
        }
      }
      voteToHalt();
    }

  }
}
