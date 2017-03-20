package in.dream_lab.goffish.giraph.examples;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Created by anirudh on 09/02/17.
 */
public class TriangleCountSubgraphValue implements Writable {
  public long triangleCount;
  public Map<Long, Set<Long>> adjSet;
  @Override
  public void write(DataOutput dataOutput) throws IOException {

  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {

  }
}
