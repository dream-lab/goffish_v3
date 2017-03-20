package in.dream_lab.goffish.giraph.factories;

import in.dream_lab.goffish.giraph.conf.GiraphSubgraphConfiguration;
import in.dream_lab.goffish.giraph.graph.SubgraphMessage;
import org.apache.giraph.conf.GiraphConfigurationSettable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.hadoop.io.Writable;

/**
 * Factory class to create default message values.
 *
 * @param <M> Message Value
 */
public class DefaultSubgraphMessageFactory<M extends Writable>
    implements MessageValueFactory<M>, GiraphConfigurationSettable {
  /**
   * Message value class
   */
  /**
   * Configuration
   */
  private GiraphSubgraphConfiguration conf;

  @Override
  public M newInstance() {
    SubgraphMessage messageValue =  ReflectionUtils.newInstance(SubgraphMessage.class);
    messageValue.initializeSubgraphId(conf.createSubgraphId());
    Writable subgraphMessageValue = conf.createSubgraphMessageValue();
    messageValue.initializeMessageValue(subgraphMessageValue);
    return (M) messageValue;
  }

  @Override
  public void setConf(ImmutableClassesGiraphConfiguration configuration) {
    this.conf = new GiraphSubgraphConfiguration(configuration);
  }
}