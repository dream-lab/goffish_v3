package in.dream_lab.goffish.giraph.formats;

import in.dream_lab.goffish.api.ISubgraph;
import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.giraph.graph.SubgraphVertices;
import in.dream_lab.goffish.giraph.graph.SubgraphId;
import org.apache.giraph.graph.*;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Created by anirudh on 26/01/17.
 */
public class SubgraphSingleSourceShortestPathOutputFormat extends
    TextVertexOutputFormat<SubgraphId<LongWritable>,SubgraphVertices,NullWritable> {
  /**
   * Simple text based vertex writer
   */
  private class SimpleTextVertexWriter extends TextVertexWriter {
    @Override
    public void writeVertex(Vertex<SubgraphId<LongWritable>, SubgraphVertices, NullWritable> vertex) throws IOException, InterruptedException {
      ISubgraph<NullWritable, LongWritable, NullWritable, LongWritable, NullWritable, LongWritable> subgraph = (ISubgraph) vertex;
      Iterable<IVertex<LongWritable, NullWritable, LongWritable, NullWritable>> vertices = subgraph.getLocalVertices();
      for (IVertex<LongWritable, NullWritable, LongWritable, NullWritable> entry : vertices) {
        getRecordWriter().write(
            new Text(String.valueOf(entry.getVertexId().get())),
            new Text(entry.getValue().toString()));
      }
    }
  }

  @Override
  public TextVertexWriter createVertexWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new SimpleTextVertexWriter();
  }
}
