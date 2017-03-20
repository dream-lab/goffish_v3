package in.dream_lab.goffish.giraph.conf;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.util.Properties;

/**
 * Created by anirudh on 13/03/17.
 */
public class GiraphSubgraphConfiguration<S extends WritableComparable,
    I extends WritableComparable, V extends Writable, E extends Writable, SV extends Writable, EI extends WritableComparable> {
  private Properties subgraphProperties;

  private ImmutableClassesGiraphConfiguration conf;
  public GiraphSubgraphConfiguration(ImmutableClassesGiraphConfiguration conf) {
    this.conf = conf;
  }

  public Long getSubgraphSourceVertex() {
    return GiraphSubgraphConstants.SUBGRAPH_SOURCE_VERTEX.get(conf);
  }

//  public SubgraphMessageValueFactory<? extends Writable> getSubgraphMessageValueFactory() {
//    return subgraphValueFactories.getSubgraphMessageValueFactory();
//  }

//  public Writable createSubgraphMessageValue() {
//    return getSubgraphMessageValueFactory().newInstance();
//  }



  public Class<? extends WritableComparable> getSubgraphIdClass() {
    return GiraphSubgraphConstants.SUBGRAPH_ID_CLASS.get(conf);
  }

  public Class<? extends WritableComparable> getSubgraphVertexIdClass() {
    return GiraphSubgraphConstants.SUBGRAPH_VERTEX_ID_CLASS.get(conf);
  }

  public Class<? extends WritableComparable> getEdgeIdClass() {
    return GiraphSubgraphConstants.SUBGRAPH_EDGE_ID_CLASS.get(conf);
  }

  public Class<? extends Writable> getSubgraphMessageValueClass() {
    return GiraphSubgraphConstants.SUBGRAPH_MESSAGE_VALUE_CLASS.get(conf);
  }

  public Class<? extends Writable> getSubgraphValueClass() {
    return GiraphSubgraphConstants.SUBGRAPH_VALUE_CLASS.get(conf);
  }

  public Class<? extends Writable> getSubgraphVertexValueClass() {
    return GiraphSubgraphConstants.SUBGRAPH_VERTEX_VALUE_CLASS.get(conf);
  }

  public Class<? extends Writable> getSubgraphVertexEdgeValueClass() {
    return GiraphSubgraphConstants.SUBGRAPH_VERTEX_EDGE_VALUE_CLASS.get(conf);
  }

  public S createSubgraphId() {
    return (S) WritableUtils.createWritable(getSubgraphIdClass(), conf);
  }

  public SV createSubgraphValue() {
    return (SV) WritableUtils.createWritable(getSubgraphValueClass(), conf);
  }

  public I createSubgraphVertexId() {
    return (I) WritableUtils.createWritable(getSubgraphVertexIdClass(), conf);
  }

  public V createSubgraphVertexValue() {
    return (V) WritableUtils.createWritable(getSubgraphVertexValueClass(), conf);
  }

  public E createEdgeValue() {
    return (E)  WritableUtils.createWritable(getSubgraphVertexEdgeValueClass());
  }

  public EI createSubgraphEdgeId() {
    return (EI) WritableUtils.createWritable(getEdgeIdClass(), conf);
  }

  public Writable createSubgraphMessageValue() {
    return WritableUtils.createWritable(getSubgraphMessageValueClass(), conf);
  }

//  public Object createObjectFromProperty(String propertyKey) throws IOException {
//    String className = subgraphProperties.getProperty(propertyKey);
//    Class<?> subgraphIdClass;
//    try {
//      subgraphIdClass = Class.forName(className);
//    } catch (ClassNotFoundException e) {
//      throw new IOException("Class not found in properties");
//    }
//    return ReflectionUtils.newInstance(subgraphIdClass);
//  }
}
