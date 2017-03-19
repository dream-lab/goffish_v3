package in.dream_lab.goffish.giraph;

import org.apache.giraph.master.DefaultMasterCompute;

/**
 * Created by anirudh on 17/03/17.
 */
public class SubgraphMasterCompute extends DefaultMasterCompute {
  public static final String ID = "SubgraphPartitionMappingAggregator";

  @Override
  public void compute() {
    long superstep = getSuperstep();
    if (superstep == 0) {
      setComputation(RemoteVerticesFinder.class);
    } else if(superstep == 1) {
      setComputation(RemoteVerticesFinder2.class);
    } else if(superstep == 2) {
      setComputation(RemoteVerticesFinder3.class);
    } else {
      setComputation(GiraphSubgraphComputation.class);
    }
  }

  @Override
  public void initialize() throws InstantiationException, IllegalAccessException {
    registerAggregator(ID, SubgraphPartitionMappingAggregator.class);
  }
}
