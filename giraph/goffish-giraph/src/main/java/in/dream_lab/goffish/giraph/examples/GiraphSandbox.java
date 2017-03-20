package in.dream_lab.goffish.giraph.examples;

import in.dream_lab.goffish.api.*;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.LinkedList;

/**
 * Created by anirudh on 08/03/17.
 */
public class GiraphSandbox extends AbstractSubgraphComputation<NullWritable, DoubleWritable, DoubleWritable, BytesWritable, LongWritable, LongWritable, LongWritable> {
  @Override
  public void compute(Iterable<IMessage<LongWritable,BytesWritable>> subgraphMessages) throws IOException {
    ISubgraph<NullWritable, DoubleWritable, DoubleWritable, LongWritable, LongWritable, LongWritable> subgraph = getSubgraph();
    for (IVertex subgraphVertex : subgraph.getLocalVertices()) {
      System.out.println("Vertex: " + subgraphVertex.getVertexId());
      Iterable<IEdge> outEdges = subgraphVertex.getOutEdges();
      for (IEdge subgraphEdge : outEdges) {
        System.out.println("Edges: " + subgraphEdge.getSinkVertexId());
      }
    }
    System.out.println("Printing remote");
    for (IVertex subgraphVertex : subgraph.getRemoteVertices()) {
      System.out.println("Remote Vertex: " + subgraphVertex.getVertexId());
    }
    voteToHalt();
  }
}
