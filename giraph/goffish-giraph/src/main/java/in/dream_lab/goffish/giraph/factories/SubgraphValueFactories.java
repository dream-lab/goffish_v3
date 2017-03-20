package in.dream_lab.goffish.giraph.factories;

import in.dream_lab.goffish.giraph.conf.GiraphSubgraphConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import static in.dream_lab.goffish.giraph.conf.GiraphSubgraphConstants.EDGE_ID_FACTORY_CLASS;

/**
 * Created by anirudh on 09/03/17.
 */
public class SubgraphValueFactories<I extends WritableComparable,
    V extends Writable, E extends Writable> {

  private final EdgeIdFactory<? extends WritableComparable> edgeIdFactory;

  private final SubgraphIdFactory<? extends WritableComparable> subgraphIdFactory;

  private final SubgraphVertexIdFactory<? extends WritableComparable> subgraphVertexIdFactory;

  private final SubgraphMessageValueFactory<? extends WritableComparable> subgraphMessageValueFactory;

  private final SubgraphValueFactory<? extends Writable> subgraphValueFactory;

  private final SubgraphVertexValueFactory<? extends Writable> subgraphVertexValueFactory;

  public SubgraphValueFactories(Configuration conf) {
    edgeIdFactory = EDGE_ID_FACTORY_CLASS.newInstance(conf);
    subgraphIdFactory = GiraphSubgraphConstants.SUBGRAPH_ID_FACTORY_CLASS.newInstance(conf);
    subgraphVertexIdFactory = GiraphSubgraphConstants.SUBGRAPH_VERTEX_ID_FACTORY_CLASS.newInstance(conf);
    subgraphMessageValueFactory = GiraphSubgraphConstants.SUBGRAPH_MESSAGE_VALUE_FACTORY_CLASS.newInstance(conf);
    subgraphValueFactory = GiraphSubgraphConstants.SUBGRAPH_VALUE_FACTORY_CLASS.newInstance(conf);
    subgraphVertexValueFactory = GiraphSubgraphConstants.SUBGRAPH_VERTEX_VALUE_FACTORY_CLASS.newInstance(conf);
  }

  public EdgeIdFactory<? extends WritableComparable> getEdgeIdFactory() {
    return edgeIdFactory;
  }

  public SubgraphIdFactory<? extends WritableComparable> getSubgraphIdFactory() {
    return subgraphIdFactory;
  }

  public SubgraphVertexIdFactory<? extends WritableComparable> getSubgraphVertexIdFactory() {
    return subgraphVertexIdFactory;
  }

  public SubgraphMessageValueFactory<? extends Writable> getSubgraphMessageValueFactory() {
    return subgraphMessageValueFactory;
  }

  public SubgraphValueFactory<? extends Writable> getSubgraphValueFactory() {
    return subgraphValueFactory;
  }

  public SubgraphVertexValueFactory<? extends Writable> getSubgraphVertexValueFactory() {
    return subgraphVertexValueFactory;
  }
}
