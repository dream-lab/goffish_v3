package in.dream_lab.goffish.giraph.factories;

import org.apache.giraph.conf.GiraphConfigurationSettable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.WritableComparable;

/**
 * Created by anirudh on 25/10/16.
 */
public class DefaultSubgraphVertexIdFactory<I extends WritableComparable> implements SubgraphVertexIdFactory<I>, GiraphConfigurationSettable {

  private ImmutableClassesGiraphConfiguration conf;
  private Class<I> subgraphVertexIdClass;

  @Override
  public I newInstance() {
    return WritableUtils.createWritable(subgraphVertexIdClass, conf);
  }

  @Override
  public void setConf(ImmutableClassesGiraphConfiguration configuration) {
      this.conf = configuration;
//      subgraphVertexIdClass = conf.getSubgraphVertexIdClass();
  }
}
