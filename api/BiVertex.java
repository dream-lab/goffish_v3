package in.dream_lab.goffish;

import java.util.Collection;

import org.apache.hadoop.io.Writable;

/* Defines Vertex interface. Could be used to define multiple graph representation,
 * e.g: adjacency list, adjacency set.
 * @param <V> Vertex value object type
 * @param <E> Edge value object type
 * */
public interface BiVertex<V extends Writable, E extends Writable, I extends Writable, J extends Writable> {
  I getVertexID();
  
  boolean isRemote();
  
  Collection<IEdge<E, J>> outEdges();
  Collection<IEdge<E, J>> InEdges();//* addition to IVertex
  
  
  V getValue();
  
  void setValue(V value);
}
