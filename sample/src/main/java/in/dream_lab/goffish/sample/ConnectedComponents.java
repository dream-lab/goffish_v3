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
package in.dream_lab.goffish.sample;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;

import in.dream_lab.goffish.api.IMessage;
import in.dream_lab.goffish.api.IRemoteVertex;
import in.dream_lab.goffish.api.AbstractSubgraphComputation;

public class ConnectedComponents extends
    AbstractSubgraphComputation<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable, LongWritable, LongWritable> {

  long minSubgraphID;

  @Override
  public void compute(
      Iterable<IMessage<LongWritable, LongWritable>> messages) throws IOException {

    if (getSuperstep() == 0) {
      minSubgraphID = getSubgraph().getSubgraphId().get();
      for (IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable> vertex : getSubgraph()
          .getRemoteVertices()) {
        if (minSubgraphID > vertex.getSubgraphId().get()) {
          minSubgraphID = vertex.getSubgraphId().get();
        }
      }
      LongWritable msg = new LongWritable(minSubgraphID);
      sendToNeighbors(msg);
      getSubgraph().setSubgraphValue(new LongWritable(minSubgraphID));
    } else {
      boolean updated = false;
      for (IMessage<LongWritable, LongWritable> msg : messages) {
        long subgraphID = msg.getMessage().get();
        if (minSubgraphID > subgraphID) {
          minSubgraphID = subgraphID;
          updated = true;
        }
      }
      if (updated) {
        getSubgraph().setSubgraphValue(new LongWritable(minSubgraphID));
        LongWritable msg = new LongWritable(minSubgraphID);
        sendToNeighbors(msg);
      }
      // System.out.println("Superstep "+ getSuperStep() + " Subgraph " +
      // getSubgraph().getSubgraphID() + " value " + getSubgraph().getValue());
    }
    // System.out.println("Subgraph " + getSubgraph().getSubgraphID() + " in
    // superstep " + getSuperStep() + " value " + getSubgraph().getValue());
    voteToHalt();
  }

}
