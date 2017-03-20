package in.dream_lab.goffish.giraph.aggregators;

import org.apache.giraph.aggregators.Aggregator;
import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.giraph.utils.ExtendedByteArrayDataInput;
import org.apache.giraph.utils.ExtendedByteArrayDataOutput;
import org.apache.hadoop.io.*;

import java.io.IOException;

/**
 * Created by anirudh on 17/03/17.
 */
public class SubgraphPartitionMappingAggregator implements Aggregator<MapWritable> {
  MapWritable subgraphPartitionMap;

  public SubgraphPartitionMappingAggregator() {
    subgraphPartitionMap = (MapWritable) createInitialValue();
  }

  @Override
  public void aggregate(MapWritable value) {
    subgraphPartitionMap.putAll(value);
  }

  @Override
  public MapWritable createInitialValue() {
    return new MapWritable();
  }

  @Override
  public MapWritable getAggregatedValue() {
    return subgraphPartitionMap;
  }

  @Override
  public void setAggregatedValue(MapWritable value) {
    subgraphPartitionMap = value;
  }

  @Override
  public void reset() {
    subgraphPartitionMap = createInitialValue();
  }
}
