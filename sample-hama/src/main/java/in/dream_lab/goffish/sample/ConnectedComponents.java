package in.dream_lab.goffish.sample;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;

import in.dream_lab.goffish.api.IMessage;
import in.dream_lab.goffish.api.IRemoteVertex;
import in.dream_lab.goffish.api.AbstractSubgraphComputation;


/**
 * Does a multi-source BFS over the meta-graph that starts at each subgraph,
 * and propagates to subgraphs whose IDs are larger than onself.
 *
 * All subgraphs that are part of the same connected component will be labeled
 * with the value of the smallest Subgraph ID in the component they belong to.
 *
 * The number of unique subgraph values are the number of components.
 *
 * @author Himanshu Sharma
 * @author Diptanshu Kakwani
 * @author Yogesh Simmhan
 *
 * @version 1.0
 * @see <a href="http://www.dream-lab.in/">DREAM:Lab</a>
 *
 *      Copyright 2017 DREAM:Lab, Indian Institute of Science, Bangalore
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
 *
 */
public class ConnectedComponents extends
    AbstractSubgraphComputation<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable, LongWritable, LongWritable> {


  // TODO: We should not store state as a field. Once sendToNeighbors() is more efficient, this can
  // be removed.
  // Maintain list of remote subgraph IDs to send messages to
  private Set<LongWritable> remoteSubgraphIDs = new HashSet<LongWritable>();

  @Override
  public void compute(Iterable<IMessage<LongWritable, LongWritable>> messages) throws IOException {

    long minSubgraphID;

    if (getSuperstep() == 0) {
      // maintain old and new subgraph IDs
      long oldSubgraphID = minSubgraphID = getSubgraph().getSubgraphId().get();
      boolean updated = false;

      // Test remote vertices to see if they have a smaller subgraph ID.
      // If so, update the minimum known ID to the remote subgraph ID.
      // TODO: We also retain the distinct remote sugraph IDs as a field to efficiently
      // sendToNeighbors()
      //
      // TODO: Check if maintaining this set is actually slower than sending
      // more messages to neighbors. e.g. ORKT
      for (IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable> vertex : getSubgraph()
          .getRemoteVertices()) {
        long sgid = vertex.getSubgraphId().get(); // add to remote subgraph set
        // if SG was is newly added, and has a smaller ID than the known ID
        if (remoteSubgraphIDs.add(new LongWritable(sgid)) && minSubgraphID > sgid) {
          minSubgraphID = sgid;
          updated = true;
        }
      }

      // Send only if my SG ID is updated
      if (updated) {
        // Send to neighbors whose IDs are larger than my new minimum ID
        // i.e. all neighbors except the min subgraph ID
        LongWritable msg = new LongWritable(minSubgraphID);
        for (LongWritable sgid : remoteSubgraphIDs) {
          if (sgid.get() != minSubgraphID) sendMessage(sgid, msg);
        }
      }
    } else { // Second and later supersteps.

      // get initial value from past iteration
      minSubgraphID = getSubgraph().getSubgraphValue().get();

      // Update my local min value if a smaller message arrives
      boolean updated = false;
      for (IMessage<LongWritable, LongWritable> msg : messages) {
        long subgraphID = msg.getMessage().get();
        if (minSubgraphID > subgraphID) {
          minSubgraphID = subgraphID;
          updated = true;
        }
      }

      // Propagate my updated local min value to all neighbors
      if (updated) {
        getSubgraph().setSubgraphValue(new LongWritable(minSubgraphID));
        LongWritable msg = new LongWritable(minSubgraphID);
        for (LongWritable sgid : remoteSubgraphIDs) {
          sendMessage(sgid, msg);
        }
      }
    }

    // update my local value
    getSubgraph().setSubgraphValue(new LongWritable(minSubgraphID));

    // All vote to halt. Will be woken up if message arrives.
    voteToHalt();
  }

}
