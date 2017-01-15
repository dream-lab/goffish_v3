package in.dream_lab.goffish.sample;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;

import in.dream_lab.goffish.SubgraphCompute;
import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IMessage;
import in.dream_lab.goffish.api.IRemoteVertex;
import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.utils.LongArrayListWritable;

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
    SubgraphCompute<LongWritable, LongWritable, LongWritable, LongArrayListWritable, LongWritable, LongWritable, LongWritable> {
  private long triangleCount, totalCount;
  StringBuilder trianglesList;
  Map<Long, Set<Long>> adjSet;

  // To represent sender and message content.
  private class Pair<L, R> {
    L first;
    R second;

    Pair(L a, R b) {
      first = a;
      second = b;
    }
  }

  @Override
  public void compute(Collection<IMessage<LongWritable, LongArrayListWritable>> messageList) {

    // Convert adjacency list to adjacency set
    if (getSuperStep() == 0) {
      //trianglesList = new StringBuilder();
      adjSet = new HashMap<Long, Set<Long>>();
      for (IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex : getSubgraph().getLocalVertices()) {
        Set<Long> adjVertices = new HashSet<Long>();
        for (IEdge<LongWritable, LongWritable, LongWritable> edge : vertex.outEdges()) {
          adjVertices.add(edge.getSinkVertexID().get());
        }
        adjSet.put(vertex.getVertexID().get(), adjVertices);
      }
      return;
    } else if (getSuperStep() == 1) {
      Map<Long, LongArrayListWritable> msg = new HashMap<Long, LongArrayListWritable>();
      for (IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex : getSubgraph()
          .getLocalVertices()) {
        for (IEdge<LongWritable, LongWritable, LongWritable> edge : vertex.outEdges()) {
          IVertex<LongWritable, LongWritable, LongWritable, LongWritable> adjVertex = 
              getSubgraph().getVertexByID(edge.getSinkVertexID());

          // Preparing messages to be sent to remote adjacent vertices.
          if (adjVertex.isRemote() && adjVertex.getVertexID().get() > vertex.getVertexID().get()) {
            long remoteSubgraphId = ((IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable>)adjVertex)
                .getSubgraphID().get();
            LongArrayListWritable vertexIds = msg.get(remoteSubgraphId);
            if (vertexIds == null) {
              vertexIds = new LongArrayListWritable();
              msg.put(remoteSubgraphId, vertexIds);
            }
            vertexIds.add(adjVertex.getVertexID());
            vertexIds.add(vertex.getVertexID());
            vertexIds.add(vertex.getVertexID());
                
          } else if (adjVertex.isRemote() || vertex.getVertexID().get() > adjVertex.getVertexID().get())
            continue;

          if (adjVertex.isRemote()) {
            continue;  //as it has no outedges
          }
          // Counting triangles which have at least two vertices in the same
          // subgraph.
          for (IEdge<LongWritable, LongWritable, LongWritable> edgeAdjVertex : adjVertex.outEdges()) {
            IVertex<LongWritable, LongWritable, LongWritable, LongWritable> adjAdjVertex = getSubgraph().getVertexByID(edgeAdjVertex.getSinkVertexID());
            if (adjAdjVertex.isRemote()
                || adjAdjVertex.getVertexID().get() > adjVertex.getVertexID().get()) {

              if (adjSet.get(vertex.getVertexID().get()).contains(adjAdjVertex.getVertexID().get())) {
                triangleCount++;
                //trianglesList.append(vertex.getVertexID().get() + " " + adjVertex.getVertexID().get()
                  //  + " " + adjAdjVertex.getVertexID().get() + "\n");
              }
            }
          }
        }
      }
      sendPackedMessages(msg);
    } else if (getSuperStep() == 2) {
      Map<Long, List<Pair<Long, Long>>> ids = new HashMap<Long, List<Pair<Long, Long>>>();
      unpackMessages(messageList, ids);

      Map<Long, LongArrayListWritable> msg = new HashMap<Long, LongArrayListWritable>();
      for (Map.Entry<Long, List<Pair<Long, Long>>> entry : ids.entrySet()) {
        IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex = getSubgraph().getVertexByID(new LongWritable(entry.getKey()));
        List<Pair<Long, Long>> idPairs = entry.getValue();
        for (IEdge<LongWritable, LongWritable, LongWritable> edge : vertex.outEdges()) {
          IVertex<LongWritable, LongWritable, LongWritable, LongWritable> adjVertex = getSubgraph().getVertexByID(edge.getSinkVertexID());
          if (adjVertex.isRemote() && adjVertex.getVertexID().get() > vertex.getVertexID().get()) {
            long remoteSubgraphId = ((IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable>)adjVertex)
                .getSubgraphID().get();
            LongArrayListWritable vertexIds = msg.get(remoteSubgraphId);
            if (vertexIds == null) {
              vertexIds = new LongArrayListWritable();
              msg.put(remoteSubgraphId, vertexIds);
            }
            for (Pair<Long, Long> id : idPairs) {
              LongWritable firstId = new LongWritable(id.first);
              IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable> sinkSubgraphID = (IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable> )
              getSubgraph().getVertexByID(firstId);
              if (sinkSubgraphID.getSubgraphID().get() != remoteSubgraphId) {
                vertexIds.add(adjVertex.getVertexID());
                vertexIds.add(firstId);
                vertexIds.add(vertex.getVertexID());
              }
            }
          }
        }
      }
      sendPackedMessages(msg);

    } else {
      Map<Long, List<Pair<Long, Long>>> ids = new HashMap<Long, List<Pair<Long, Long>>>();
      unpackMessages(messageList, ids);
      for (Map.Entry<Long, List<Pair<Long, Long>>> entry : ids.entrySet()) {
        IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex = getSubgraph().getVertexByID(new LongWritable(entry.getKey()));
        for (Pair<Long, Long> p : entry.getValue()) {
          for (IEdge<LongWritable, LongWritable, LongWritable> edge : vertex.outEdges()) {
            if (edge.getSinkVertexID().get() == p.first) {
              triangleCount++;
              //trianglesList.append(
               //   vertex.getVertexID().get() + " " + p.first + " " + p.second + "\n");
            }
          }
        }

      }
    }
    getSubgraph().setValue(new LongWritable(triangleCount));
    voteToHalt();
  }

  void sendPackedMessages(Map<Long, LongArrayListWritable> msg) {
    for (Map.Entry<Long, LongArrayListWritable> m : msg.entrySet()) {
      if (!m.getValue().isEmpty()) {
        sendMessage(new LongWritable(m.getKey()), m.getValue());
      }
    }
  }

  /*
   * Unpacks the messages such that there is a list of pair of message vertex id
   * and source vertex Ids associated with the each target vertex.
   */
  void unpackMessages(Collection<IMessage<LongWritable, LongArrayListWritable>> messageList,
      Map<Long, List<Pair<Long, Long>>> ids) {
    for (IMessage<LongWritable, LongArrayListWritable> messageItem : messageList) {
      LongArrayListWritable message = messageItem.getMessage();
      for (int i = 0; i < message.size(); i += 3) {
        Long targetId = ((LongWritable)message.get(i)).get();
        Long messageId = ((LongWritable)message.get(i + 1)).get();
        Long sourceId = ((LongWritable)message.get(i + 2)).get();
     
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
