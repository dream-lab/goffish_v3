package in.dream_lab.goffish;

import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/*
 * @param <S> Subgraph value object type
 * @param <V> Vertex value object type
 * @param <E> Edge value object type
 * @param <M> Message object type
 * */
public interface SubgraphInterface<S extends Writable, V extends Writable, E extends Writable, M extends Writable> {
    
  Vertex getVertexByID(long vertexID);

  long getSubgraphID();

  long vertexCount();
  
  long localVertexCount();

  void voteToHalt();

  boolean hasVotedToHalt();

  List<Vertex> getVertices();
  
  List<Vertex> getLocalVertices();

  long getSuperStep();

  int getPartitionID();

  void compute(List<M> messages);
  
  void sendMessage(long subgraphID, M message);
  
  void sendToAll(M message);
  
  void sendToNeighbors(M message);
}
