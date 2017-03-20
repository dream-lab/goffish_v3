package in.dream_lab.goffish.giraph.formats;

import in.dream_lab.goffish.api.ISubgraph;
import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.giraph.examples.ShortestPathSubgraphValue;
import in.dream_lab.goffish.giraph.graph.SubgraphVertices;
import in.dream_lab.goffish.giraph.graph.SubgraphId;
import org.apache.giraph.graph.*;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Map;

/**
 * Created by anirudh on 26/01/17.
 */
public class SubgraphSingleSourceShortestPathOutputFormatSir extends
    TextVertexOutputFormat<SubgraphId<LongWritable>,SubgraphVertices,NullWritable> {
  /**
   * Simple text based vertex writer
   */
  private class SimpleTextVertexWriter extends TextVertexWriter {
    @Override
    public void writeVertex(Vertex<SubgraphId<LongWritable>, SubgraphVertices, NullWritable> vertex) throws IOException, InterruptedException {
      ISubgraph<ShortestPathSubgraphValue, LongWritable, NullWritable, LongWritable, NullWritable, LongWritable> subgraph = (ISubgraph) vertex;
      Map<Long, Short> shortestDistanceMap = subgraph.getSubgraphValue().shortestDistanceMap;
      for (Map.Entry<Long, Short> entry : shortestDistanceMap.entrySet()) {
        IVertex<LongWritable, NullWritable, LongWritable, NullWritable> subgraphVertex = subgraph.getVertexById(new LongWritable(entry.getKey()));
        if (!subgraphVertex.isRemote()) {
          getRecordWriter().write(
              new Text(entry.getKey().toString()),
              new Text(entry.getValue().toString()));
        }
      }
    }
  }

  @Override
  public TextVertexWriter createVertexWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new SimpleTextVertexWriter();
  }
}