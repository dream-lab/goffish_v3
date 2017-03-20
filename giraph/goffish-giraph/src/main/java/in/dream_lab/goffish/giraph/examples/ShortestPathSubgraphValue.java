package in.dream_lab.goffish.giraph.examples;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * Created by anirudh on 10/02/17.
 */
public class ShortestPathSubgraphValue implements Writable
{
  public Map<Long, Short> shortestDistanceMap;

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    throw new UnsupportedOperationException();
  }
}
