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
import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.google.common.collect.Iterables;

import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IMessage;
import in.dream_lab.goffish.api.IRemoteVertex;
import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.api.AbstractSubgraphComputation;

/*
 * ported from goffish v2
 */

/**
 * Yogesh: Updated to reduce hashmap puts, improved string perf
 */
public class PageRank extends
    AbstractSubgraphComputation<LongWritable, LongWritable, LongWritable, Text, LongWritable, LongWritable, LongWritable> {

  private static final int PAGERANK_LOOPS = 1;

  private final class MyDouble {
    public double d;
  }

  public PageRank() {
  }

  private Map<Long, Double> _weights;
  private Map<Long, MyDouble> sums;

  @Override
  public void compute(Iterable<IMessage<LongWritable, Text>> messages) throws IOException {
    if (getSuperstep() == 0) {
      _weights = new HashMap<>((int) getSubgraph().getVertexCount(), 1f);
      sums = new HashMap<>((int) getSubgraph().getVertexCount(), 1f);

      // initialize weights
      // initialize sums
      for (IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex : getSubgraph()
          .getVertices()) {
        _weights.put(vertex.getVertexId().get(), 1D);
        sums.put(vertex.getVertexId().get(), new MyDouble());
      }
    }


    MyDouble myD;
    for (int i = 0; i < PAGERANK_LOOPS; i++) {

      // calculate sums from this subgraphs
      for (IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex : getSubgraph()
          .getVertices()) {
        if (vertex.isRemote()) {
          // weights from remote vertices are sent through messages
          continue;
        }

        double delta = _weights.get(vertex.getVertexId().get())
            / Iterables.size(vertex.getOutEdges());
        for (IEdge<LongWritable, LongWritable, LongWritable> edge : vertex
            .getOutEdges()) {
          myD = sums.get(edge.getSinkVertexId().get()); // sinkVertexId
          myD.d += delta;
        }
      }

      // add in sums from remote vertices
      for (IMessage<LongWritable, Text> message : messages) {
        // fixme: use inline string.split for even better perf
        // use ":" as global separator for better perf. O(s) complexity, where s
        // is string length
        String[] parts = message.getMessage().toString().split(":");
        for (i = 0; i < parts.length; i++) { // String part : parts
          long vertexId = Long.parseLong(parts[i]);
          i++;
          double delta = Double.parseDouble(parts[i]);
          myD = sums.get(vertexId);
          myD.d += delta;
        }
      }

      // update weights
      for (IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex : getSubgraph()
          .getVertices()) {
        if (vertex.isRemote()) {
          continue;
        }
        myD = sums.get(vertex.getVertexId().get());
        double pr = 0.15 + 0.85 * myD.d;
        _weights.put(vertex.getVertexId().get(), pr);
        // set sum of non-remote vertices to zero here to avoid doing it at
        // start of next superstep
        myD.d = 0d;
      }
    }

    if (getSuperstep() < 30) {

      // message aggregation
      HashMap<Long, StringBuilder> messageAggregator = new HashMap<>(
          (int) (getSubgraph().getVertexCount()
              - getSubgraph().getLocalVertexCount()),
          1f);
      for (IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable> remoteVertex : getSubgraph()
          .getRemoteVertices()) {
        StringBuilder b = messageAggregator.get(remoteVertex.getSubgraphId());
        if (b == null) {
          b = new StringBuilder();
          messageAggregator.put(remoteVertex.getSubgraphId().get(), b);
        }

        myD = sums.get(remoteVertex.getVertexId().get());
        b.append(remoteVertex.getVertexId().get()).append(':').append(myD.d)
            .append(':');
        // set sum of remote vertices to zero here to avoid doing it at start of
        // next superstep
        myD.d = 0d;
      }

      // send outgoing weights to remote edges
      for (Map.Entry<Long, StringBuilder> entry : messageAggregator
          .entrySet()) {
        Text message = new Text(entry.getValue().toString());
        sendMessage(new LongWritable(entry.getKey()), message);
      }

    } else {
      voteToHalt();
      System.out.println("voting to halt");

      for (IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex : getSubgraph()
          .getVertices()) {
        if (!vertex.isRemote()) {
          System.out.println(vertex.getVertexId().get() + " "
              + _weights.get(vertex.getVertexId().get())
              + System.lineSeparator());
        }
      }
    }
  }

  // http://hg.openjdk.java.net/jdk7/jdk7/jdk/rev/1ff977b938e5
  public static final String split(String source, char ch) {
    int off = 0;
    int next = 0;
    while ((next = source.indexOf(ch, off)) != -1) {
      return source.substring(off, next);
      // off = next + 1;
    }
    return null;
  }
}
