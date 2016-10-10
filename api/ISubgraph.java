package in.dream_lab.goffish;

import java.util.Collection;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/*TODO: Add IWCSubgraph and ISCSubgraph
 * @param <S> Subgraph value object type
 * @param <V> Vertex value object type
 * @param <E> Edge value object type
 * @param <M> Message object type
 * */
public interface ISubgraph<S extends Writable, V extends Writable, E extends Writable, I extends Writable, J extends Writable, K extends Writable> {
    
  IVertex<V, E, I, J> getVertexByID(I vertexID);

  K getSubgraphID();

  long vertexCount();
  
  long localVertexCount();

  Collection<IVertex<V, E, I, J>> getVertices();
  
  Collection<IVertex<V, E, I, J>> getLocalVertices();
  
  Collection<IRemoteVertex<V, E, I, J, K>> getRemoteVertices();
  
  void setValue(S value);
  
  S getValue();
}
