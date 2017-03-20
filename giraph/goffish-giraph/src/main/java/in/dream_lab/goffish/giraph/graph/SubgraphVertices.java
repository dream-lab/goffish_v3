package in.dream_lab.goffish.giraph.graph;

import in.dream_lab.goffish.api.IRemoteVertex;
import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.giraph.conf.GiraphSubgraphConfiguration;
import in.dream_lab.goffish.giraph.graph.DefaultRemoteSubgraphVertex;
import in.dream_lab.goffish.giraph.graph.DefaultSubgraphVertex;
import org.apache.giraph.conf.GiraphConfigurationSettable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Created by anirudh on 27/09/16.
 *
 * @param <K> Subgraph id
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */

public class SubgraphVertices<S extends Writable, V extends Writable, E extends Writable, I extends WritableComparable, J extends WritableComparable, K extends WritableComparable
    > implements Writable, GiraphConfigurationSettable {

  private HashMap<I, IRemoteVertex<V, E, I, J, K>> remoteVertices;
  private S subgraphValue;
  private HashMap<I, IVertex<V, E, I, J>> vertices;

  private ImmutableClassesGiraphConfiguration conf;

  MapWritable subgraphParitionMapping;

  public SubgraphVertices() {
////        System.out.println("Calling subgraph vertices constructor");
//        try {
////            System.out.println("Inside try");
//            throw new Exception("Calling constructor");
//        } catch(Exception e) {
////            System.out.println("Inside catch");
////            e.printStackTrace(System.out);
//            e.printStackTrace();
//        }
  }

  public HashMap<I, IRemoteVertex<V, E, I, J, K>> getRemoteVertices() {
    return remoteVertices;
  }

  public long getNumRemoteVertices() {
    return (long) remoteVertices.size();
  }

  public void setRemoteVertices(HashMap<I, IRemoteVertex<V, E, I, J, K>> remoteVertices) {
    this.remoteVertices = remoteVertices;
  }

  public Iterable<IVertex<V, E, I, J>> getVertices() {
    return new Iterable<IVertex<V, E, I, J>>() {

      private Iterator<IVertex<V, E, I, J>> localVertexIterator = vertices.values().iterator();
      private Iterator<IRemoteVertex<V, E, I, J, K>> remoteVertexIterator = remoteVertices.values().iterator();

      @Override
      public Iterator<IVertex<V, E, I, J>> iterator() {
        return new Iterator<IVertex<V, E, I, J>>() {
          @Override
          public boolean hasNext() {
            if (localVertexIterator.hasNext()) {
              return true;
            } else {
              return remoteVertexIterator.hasNext();
            }
          }

          @Override
          public IVertex<V, E, I, J> next() {
            if (localVertexIterator.hasNext()) {
              return localVertexIterator.next();
            } else {
              return remoteVertexIterator.next();
            }
          }

          @Override
          public void remove() {

          }
        };
      }
    };
  }

  public S getSubgraphValue() {
    return subgraphValue;
  }

  public void setSubgraphValue(S subgraphValue) {
    this.subgraphValue = subgraphValue;
  }

  public long getNumVertices() {
    return vertices.size();
  }

  public HashMap<I, IVertex<V, E, I, J>> getLocalVertices() {
    return vertices;
  }

  public IVertex<V, E, I, J> getVertexById(I vertexId) {
    IVertex<V, E, I, J> subgraphVertex = vertices.get(vertexId);
    return subgraphVertex != null ? subgraphVertex : remoteVertices.get(vertexId);
  }

  public void initialize(HashMap<I, IVertex<V, E, I, J>> vertices) {
    this.vertices = vertices;
    this.remoteVertices = new HashMap<>();
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
//    System.out.println("Write Subgraph Value:" + subgraphValue + "\t"+ subgraphValue.getClass().getSimpleName());
    subgraphValue.write(dataOutput);
    dataOutput.writeInt(vertices.size());
    for (IVertex<V, E, I, J> vertex : vertices.values()) {
      ((DefaultSubgraphVertex)vertex).write(dataOutput);
    }
    dataOutput.writeInt(remoteVertices.size());
    for (IRemoteVertex<V, E, I, J, K> vertex : remoteVertices.values()) {
      ((DefaultRemoteSubgraphVertex)vertex).write(dataOutput);
    }
//    System.out.println("Write Num Vertices:" + vertices.size());
  }

  public void readFields(DataInput dataInput) throws IOException {
    GiraphSubgraphConfiguration<K,I,V,E, S, J> giraphSubgraphConfiguration = new GiraphSubgraphConfiguration(conf);
    subgraphValue = giraphSubgraphConfiguration.createSubgraphValue();
    subgraphValue.readFields(dataInput);
    int numVertices = dataInput.readInt();
//    System.out.println("Read Subgraph Value:" + subgraphValue + "\t"+ subgraphValue.getClass().getSimpleName());
//    System.out.println("Read Num Vertices:" + numVertices);
    vertices = new HashMap<>();
    for (int i = 0; i < numVertices; i++) {
      DefaultSubgraphVertex<V, E, I, J> subgraphVertex = new DefaultSubgraphVertex<V, E, I, J>();
      subgraphVertex.readFields(giraphSubgraphConfiguration, dataInput);
      vertices.put(subgraphVertex.getVertexId(), subgraphVertex);
    }
    remoteVertices = new HashMap<>();
    int numRemoteVertices = dataInput.readInt();
    for (int i = 0; i < numRemoteVertices; i++) {
      DefaultRemoteSubgraphVertex<V, E, I, J, K> remoteSubgraphVertex = new DefaultRemoteSubgraphVertex<>();
      remoteSubgraphVertex.readFields(giraphSubgraphConfiguration, dataInput);
      remoteVertices.put(remoteSubgraphVertex.getVertexId(), remoteSubgraphVertex);
    }
  }

  @Override
  public void setConf(ImmutableClassesGiraphConfiguration configuration) {
    conf = configuration;
  }

  public void setSubgraphParitionMapping(MapWritable subgraphParitionMapping) {
    this.subgraphParitionMapping = subgraphParitionMapping;
  }

  public MapWritable getSubgraphParitionMapping() {
    return subgraphParitionMapping;
  }
}
