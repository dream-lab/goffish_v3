package in.dream_lab.goffish.giraph.factories;

import org.apache.giraph.conf.GiraphConfigurationSettable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.WritableComparable;

/**
 * Created by anirudh on 25/10/16.
 */
public class DefaultSubgraphIdFactory<S extends WritableComparable> implements SubgraphIdFactory<S>, GiraphConfigurationSettable {

  private ImmutableClassesGiraphConfiguration conf;
  private Class<S> subgraphIdClass;

  @Override
  public S newInstance() {
    return WritableUtils.createWritable(subgraphIdClass, conf);
  }

  @Override
  public void setConf(ImmutableClassesGiraphConfiguration configuration) {
    this.conf = configuration;
//    subgraphIdClass = conf.getSubgraphIdClass();
  }
}
