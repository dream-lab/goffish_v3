package in.dream_lab.goffish.giraph;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

/**
 * Created by anirudh on 17/03/17.
 */
public class SubgraphPartitionMappingWritable implements Writable {
  public HashMap<LongWritable, IntWritable> subgraphPartitionMap = new HashMap<>();
  @Override
  public void write(DataOutput dataOutput) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    throw new UnsupportedOperationException();
  }
}
