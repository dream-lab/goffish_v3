package in.dream_lab.goffish.giraph.examples;

/**
 * Created by anirudh on 06/02/17.
 */

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import in.dream_lab.goffish.api.*;
import org.apache.giraph.utils.ExtendedByteArrayDataInput;
import org.apache.giraph.utils.ExtendedByteArrayDataOutput;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

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

public class SubgraphTriangleCount extends
    AbstractSubgraphComputation<TriangleCountSubgraphValue, LongWritable, NullWritable, BytesWritable, LongWritable, LongWritable, LongWritable> {

  @Override
  public void compute(Iterable<IMessage<LongWritable,BytesWritable>> subgraphMessages) throws IOException {
    // Convert adjacency list to adjacency set
    ISubgraph<TriangleCountSubgraphValue, LongWritable, NullWritable, LongWritable, LongWritable, LongWritable> subgraph = getSubgraph();
    if (getSuperstep() == 0) {
      TriangleCountSubgraphValue triangleCountSubgraphValue = new TriangleCountSubgraphValue();
      triangleCountSubgraphValue.adjSet = new HashMap<Long, Set<Long>>();
      subgraph.setSubgraphValue(triangleCountSubgraphValue);
      for (IVertex<LongWritable, NullWritable, LongWritable, LongWritable> vertex : subgraph.getLocalVertices()) {
        Set<Long> adjVertices = new HashSet<Long>();
        for (IEdge<NullWritable, LongWritable, LongWritable> edge : vertex.getOutEdges()) {
          adjVertices.add(edge.getSinkVertexId().get());
        }
        triangleCountSubgraphValue.adjSet.put(vertex.getVertexId().get(), adjVertices);
      }
      return;
    }
    else if(getSuperstep() == 1) {
      TriangleCountSubgraphValue triangleCountSubgraphValue = subgraph.getSubgraphValue();
      long triangleCount = triangleCountSubgraphValue.triangleCount;
      Map<LongWritable, ExtendedByteArrayDataOutput> msg = new HashMap<>();
      for (IVertex<LongWritable, NullWritable, LongWritable, LongWritable> vertex : subgraph.getLocalVertices()) {
        for (IEdge<NullWritable, LongWritable, LongWritable> edge : vertex.getOutEdges()) {
          IVertex<LongWritable, NullWritable, LongWritable, LongWritable> adjVertex =
              subgraph.getVertexById(edge.getSinkVertexId());

          // Preparing messages to be sent to remote adjacent vertices.
          if (adjVertex.isRemote() && adjVertex.getVertexId().get() > vertex.getVertexId().get()) {
            LongWritable remoteSubgraphId = ((IRemoteVertex<LongWritable, NullWritable, LongWritable, LongWritable, LongWritable>) adjVertex)
                .getSubgraphId();
            ExtendedByteArrayDataOutput vertexIds = msg.get(remoteSubgraphId);
            if (vertexIds == null) {
              vertexIds = new ExtendedByteArrayDataOutput();
              msg.put(remoteSubgraphId, vertexIds);
            }
            vertexIds.writeLong(adjVertex.getVertexId().get());
            vertexIds.writeLong(vertex.getVertexId().get());
            vertexIds.writeLong(vertex.getVertexId().get());

          } else if (adjVertex.isRemote() || vertex.getVertexId().get() > adjVertex.getVertexId().get())
            continue;

          if (adjVertex.isRemote()) {
            continue;  //as it has no outedges
          }
          // Counting triangles which have at least two vertices in the same
          // subgraph.
          for (IEdge<NullWritable, LongWritable, LongWritable> edgeAdjVertex : adjVertex.getOutEdges()) {
            IVertex<LongWritable, NullWritable, LongWritable, LongWritable> adjAdjVertex = subgraph.getVertexById(edgeAdjVertex.getSinkVertexId());
            if (adjAdjVertex.isRemote()
                || adjAdjVertex.getVertexId().get() > adjVertex.getVertexId().get()) {
              if (triangleCountSubgraphValue.adjSet.get(vertex.getVertexId().get()).contains(adjAdjVertex.getVertexId().get())) {
                triangleCount++;
                //trianglesList.append(vertex.getVertexID().get() + " " + adjVertex.getVertexID().get()
                //  + " " + adjAdjVertex.getVertexID().get() + "\n");
              }
            }
          }
        }
      }
      triangleCountSubgraphValue.triangleCount = triangleCount;
      sendPackedMessages(msg);
    } else if (getSuperstep() == 2) {
      Map<Long, List<Pair<Long, Long>>> ids = new HashMap<Long, List<Pair<Long, Long>>>();
      unpackMessages(subgraphMessages, ids);

      Map<LongWritable, ExtendedByteArrayDataOutput> msg = new HashMap<>();
      for (Map.Entry<Long, List<Pair<Long, Long>>> entry : ids.entrySet()) {
        IVertex<LongWritable, NullWritable, LongWritable, LongWritable> vertex = subgraph.getVertexById(new LongWritable(entry.getKey()));
        List<Pair<Long, Long>> idPairs = entry.getValue();
        for (IEdge<NullWritable, LongWritable, LongWritable> edge : vertex.getOutEdges()) {
          IVertex<LongWritable, NullWritable, LongWritable, LongWritable> adjVertex = subgraph.getVertexById(edge.getSinkVertexId());
          if (adjVertex.isRemote() && adjVertex.getVertexId().get() > vertex.getVertexId().get()) {
            LongWritable remoteSubgraphId = ((IRemoteVertex<LongWritable, NullWritable, LongWritable, LongWritable, LongWritable>) adjVertex)
                .getSubgraphId();
            ExtendedByteArrayDataOutput vertexIds = msg.get(remoteSubgraphId);
            if (vertexIds == null) {
              vertexIds = new ExtendedByteArrayDataOutput();
              msg.put(remoteSubgraphId, vertexIds);
            }
            for (Pair<Long, Long> id : idPairs) {
              LongWritable firstId = new LongWritable(id.first);
              IRemoteVertex<LongWritable, NullWritable, LongWritable, LongWritable, LongWritable> sinkSubgraphID = (IRemoteVertex<LongWritable, NullWritable, LongWritable, LongWritable, LongWritable>)
                  subgraph.getVertexById(firstId);
              if (sinkSubgraphID.getSubgraphId() != remoteSubgraphId) {
                vertexIds.writeLong(adjVertex.getVertexId().get());
                vertexIds.writeLong(firstId.get());
                vertexIds.writeLong(vertex.getVertexId().get());
              }
            }
          }
        }
      }
      sendPackedMessages(msg);

    } else {
      long triangleCount = subgraph.getSubgraphValue().triangleCount;
      Map<Long, List<Pair<Long, Long>>> ids = new HashMap<Long, List<Pair<Long, Long>>>();
      unpackMessages(subgraphMessages, ids);
      for (Map.Entry<Long, List<Pair<Long, Long>>> entry : ids.entrySet()) {
        IVertex<LongWritable, NullWritable, LongWritable, LongWritable> vertex = subgraph.getVertexById(new LongWritable(entry.getKey()));
        for (Pair<Long, Long> p : entry.getValue()) {
          for (IEdge<NullWritable, LongWritable, LongWritable> edge : vertex.getOutEdges()) {
            if (edge.getSinkVertexId().get() == p.first) {
              triangleCount++;
            }
          }
        }

      }
      subgraph.getSubgraphValue().triangleCount = triangleCount;
    }
    voteToHalt();
  }

  // To represent sender and message content.
  private class Pair<L, R> {
    L first;
    R second;

    Pair(L a, R b) {
      first = a;
      second = b;
    }
  }

  void sendPackedMessages(Map<LongWritable, ExtendedByteArrayDataOutput> msg) throws IOException {
    for (Map.Entry<LongWritable, ExtendedByteArrayDataOutput> m : msg.entrySet()) {
      m.getValue().writeLong(-1);
      sendMessage(m.getKey(), new BytesWritable(m.getValue().getByteArray()));
    }
  }

  /*
   * Unpacks the messages such that there is a list of pair of message vertex id
   * and source vertex Ids associated with the each target vertex.
   */
  void unpackMessages(Iterable<IMessage<LongWritable,BytesWritable>> subgraphMessages,
                      Map<Long, List<Pair<Long, Long>>> ids) throws IOException {
    for (IMessage<LongWritable,BytesWritable> messageItem : subgraphMessages) {
      BytesWritable message = messageItem.getMessage();
      ExtendedByteArrayDataInput dataInput = new ExtendedByteArrayDataInput(message.getBytes());
      Long targetId;
      while((targetId=dataInput.readLong()) != -1) {
        Long messageId = dataInput.readLong();
        Long sourceId = dataInput.readLong();
        List<Pair<Long, Long>> idPairs = ids.get(targetId);
        if (idPairs == null) {
          idPairs = new LinkedList<Pair<Long, Long>>();
          ids.put(targetId, idPairs);
        }
        idPairs.add(new Pair<Long, Long>(messageId, sourceId));
      }
    }
  }

}