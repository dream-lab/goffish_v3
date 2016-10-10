package in.dream_lab.goffish;

import java.util.Collection;

import org.apache.hadoop.io.Writable;

/* Defines Vertex interface. Could be used to define multiple graph representation,
 * e.g: adjacency list, adjacency set.
 * @param <V> Vertex value object type
 * @param <E> Edge value object type
 * */
public interface IRemoteVertex<V extends Writable, E extends Writable, I extends Writable, J extends Writable, K extends Writable>  extends IVertex<V, E, I, J> {
  I getVertexID();
  
  boolean isRemote();
  
  Collection<IEdge<E, I, J>> outEdges();
  
  V getValue();
  
  void setValue(V value);
  
  J getSubgraphID();
}
