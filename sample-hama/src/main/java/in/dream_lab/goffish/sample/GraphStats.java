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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IMessage;
import in.dream_lab.goffish.api.IRemoteVertex;
import in.dream_lab.goffish.api.ISubgraph;
import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.api.AbstractSubgraphComputation;

/**
 * Prints:
 * 
 * Vertex count
 * edge count
 * number of subgraphs
 * number of boundary vertices
 * diameter (meta graph)
 * each subgraph vertex and edge (local and remote) count
 * Neighbor subgraph id's
 * 
 * @author humus
 *
 */

// TODO: Meta graph Diameter is always 1 more than the actual diameter as the
// diameter count starts from 2.. change it to 1 and resolve any other conflicts
// due to this (if any)
public class GraphStats extends
    AbstractSubgraphComputation<LongWritable, LongWritable, LongWritable, Text, LongWritable, LongWritable, LongWritable> {

  private long _vertexCount = 0;
  private long _edgeCount = 0;
  private long _subgraphCount = 0;
  private long _metaGraphDiameter = 1;
  private Map<Long, Long> _distanceMap = new HashMap<>();
  private List<Long> _probed = new ArrayList<>();
  private List<Long> _boundaryVertices = Lists.newArrayList();
  private HashSet<Long> _neighbours = Sets.newHashSet();

  @Override
  public void compute(Iterable<IMessage<LongWritable, Text>> messages)
      throws IOException {

    if (getSuperstep() == 0) {
      long vertexCount = getSubgraph().getLocalVertexCount();
      long edgeCount = Iterables.size(getSubgraph().getOutEdges());
      String msgString = vertexCount + ";" + edgeCount;
      Text message = new Text(msgString);
      sendToAll(message);
      
      //For Meta graph adjacency list
      for (IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable> remote : getSubgraph()
          .getRemoteVertices()) {
        _neighbours.add(remote.getSubgraphId().get());
      }

      // For finding boundary Vertices
      for (IVertex<LongWritable, LongWritable, LongWritable, LongWritable> v :getSubgraph().getLocalVertices()) {
Inner:  for (IEdge<LongWritable, LongWritable, LongWritable> e : v.getOutEdges()) {
          if (getSubgraph().getVertexById(e.getSinkVertexId()).isRemote()) {
            _boundaryVertices.add(v.getVertexId().get());
            break Inner;
          }
        }
      }

      return;
    }

    if (getSuperstep() == 1) {
      for (IMessage<LongWritable, Text> message : messages) {
        // number of messages received = number of subgraphs
        _subgraphCount++;
        String msgString = message.getMessage().toString();
        String msgStringarr[] = msgString.split(";");
        _vertexCount += Long.parseLong(msgStringarr[0]);
        _edgeCount += Long.parseLong(msgStringarr[1]);
      }
      System.out.println(_vertexCount + ";" + _edgeCount);
      return;
    }

    // Find Diameter of meta graph from this superstep

    if (getSuperstep() == 2) {
      // create a probe message(messageType;subgraphid;distance to next node)
      // P = Probe Message
      // R = Reply Message
      String msg = "P;" + getSubgraph().getSubgraphId().get() + ";" + 2;
      _probed.add(getSubgraph().getSubgraphId().get());
      // distance to itself is 1
      _distanceMap.put(getSubgraph().getSubgraphId().get(), new Long(1));
      Text probeMessage = new Text(msg);
      sendToNeighbors(probeMessage);
      sendToAll(new Text("HasUpdates"));
      return;
    }

    /*
     * true if any probe forwarded by this subgraph (signifies the algo is
     * progressing), i.e., we have update
     */
    boolean hasUpdates = false;
    /*
     * true if any other subgraph is still alive ,i.e. , algo still progressing
     */
    boolean progressing = false;
    /*
     * when all values have been computed call the cleanup procedure to display
     * output
     */
    boolean callCleanup = false;

    for (IMessage<LongWritable, Text> recievedMessage : messages) {

      if (recievedMessage.getMessage().toString().equals("HasUpdates")) {
        progressing = true;
        continue;
      }

      if (recievedMessage.getMessage().charAt(0) == 'D') {
        // Diameter broadcast
        String msgArr[] = recievedMessage.getMessage().toString().split(";");
        Long recievedDiameter = Long.parseLong(msgArr[1]);
        if (recievedDiameter > _metaGraphDiameter) {
          _metaGraphDiameter = recievedDiameter;
        }
        callCleanup = true;
        continue;
      }

      String msg = recievedMessage.getMessage().toString();
      String msgArr[] = msg.split(";");
      Long subgraphID = Long.parseLong(msgArr[1]);
      Long distance = Long.parseLong(msgArr[2]);
      if (msgArr[0].equals("P")) {
        if (_probed.contains(subgraphID)) {
          // already probed
          continue;
        }
        hasUpdates = true;
        _probed.add(subgraphID);
        // forward the probe to neighbours
        String forwardProbeMsg = "P;" + subgraphID + ";" + (distance + 1);
        sendToNeighbors(new Text(forwardProbeMsg));
        // reply with the distance to the probe initiator
        String replyMsg = "R;" + getSubgraph().getSubgraphId().get() + ";"
            + distance;
        sendMessage(new LongWritable(subgraphID), new Text(replyMsg));
      }
      // if this the Reply message add it to the map
      else {
        _distanceMap.put(subgraphID, distance);
      }
    }

    if (hasUpdates) {
      sendToAll(new Text("HasUpdates"));
    }

    if (callCleanup) {
      cleanup();
      voteToHalt();
      return;
    }

    // if values have converged
    if (!hasUpdates && !progressing) {
      // broadcast the max diameter to everyone
      for (Map.Entry<Long, Long> subgraphDistancepair : _distanceMap
          .entrySet()) {
        if (subgraphDistancepair.getValue() > _metaGraphDiameter) {
          _metaGraphDiameter = subgraphDistancepair.getValue();
        }
      }
      String msg = "D;" + _metaGraphDiameter;
      sendToAll(new Text(msg));
    }

    // voteToHalt();
  }

  private void cleanup() {
    System.out.println("Vertex Count = " + _vertexCount);
    System.out.println("Edge Count = " + _edgeCount);
    System.out.println("Subgraph Count = " + _subgraphCount);
    System.out.println("Meta Graph Diameter = " + _metaGraphDiameter);
    ISubgraph<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable, LongWritable> subgraph = getSubgraph();
    System.out.println("Subgraph " + subgraph.getSubgraphId()+" has " + _boundaryVertices.size() + " boundary vertices");
    System.out.println("Subgraph " + subgraph.getSubgraphId()+" has " + subgraph.getLocalVertexCount() +" local vertices");
    System.out.println("Subgraph " + subgraph.getSubgraphId()+" has " + (subgraph.getVertexCount() - subgraph.getLocalVertexCount()) +" remote vertices");
    System.out.println("Subgraph " + subgraph.getSubgraphId() + " has " + Iterables.size(subgraph.getOutEdges()) + " edges");
    if (_neighbours.size() > 0) {
      System.out.print("Subgraph " + subgraph.getSubgraphId() + " has neighbours ");
    } else {
      System.out.println("Subgraph " + subgraph.getSubgraphId() + " has no neighbours ");
    }
    for (Long neighbour : _neighbours) {
      System.out.print(neighbour + " ");
    }
    System.out.println();
  }

}
