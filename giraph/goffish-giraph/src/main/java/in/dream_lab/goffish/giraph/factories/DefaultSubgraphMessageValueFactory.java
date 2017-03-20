package in.dream_lab.goffish.giraph.factories;

import org.apache.giraph.conf.GiraphConfigurationSettable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.Writable;

/**
 * Created by anirudh on 29/11/16.
 */
public class DefaultSubgraphMessageValueFactory<M extends Writable>  implements SubgraphMessageValueFactory<M>, GiraphConfigurationSettable {

  private ImmutableClassesGiraphConfiguration conf;
  private Class<M> subgraphMessageValueClass;

  @Override
  public M newInstance() {
    return org.apache.giraph.utils.WritableUtils.createWritable(subgraphMessageValueClass,conf);
  }
  public void setConf(ImmutableClassesGiraphConfiguration configuration) {
    this.conf = configuration;
//    subgraphMessageValueClass = conf.getSubgraphMessageValueClass();
  }
}
