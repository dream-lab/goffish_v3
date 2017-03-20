package in.dream_lab.goffish.giraph.graph;

import com.google.common.collect.Lists;
import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.giraph.conf.GiraphSubgraphConfiguration;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;

/**
 * Created by anirudh on 27/09/16.
 */
public class DefaultSubgraphVertex<V extends Writable, E extends Writable, I extends WritableComparable,
    J extends WritableComparable> implements IVertex<V, E, I, J>, WritableComparable {


  private I id;
  private V value;
  private LinkedList<IEdge<E, I, J>> outEdges;

  @Override
  public LinkedList<IEdge<E, I, J>> getOutEdges() {
    return outEdges;
  }

  @Override
  public int compareTo(Object o) {
    DefaultSubgraphVertex other = (DefaultSubgraphVertex) o;
    return id.compareTo(other.id);
  }

  @Override
  public I getVertexId() {
    return id;
  }

  public void setId(I id) {
    this.id = id;
  }

  @Override
  public V getValue() {
    return value;
  }

  @Override
  public void setValue(V value) {
    this.value = value;
  }

  @Override
  public boolean isRemote() {
    return false;
  }

  public void initialize(I vertexId, V value, LinkedList<IEdge<E, I, J>> edges) {
    this.id = vertexId;
    this.value = value;
    this.outEdges = edges;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
//        System.out.println("Write: " + "ID:" + id + id.getClass().getSimpleName());
//        System.out.println("Write: " + "Value:" + value + value.getClass().getSimpleName());
    id.write(dataOutput);
    value.write(dataOutput);
    int numOutEdges = outEdges.size();
    dataOutput.writeInt(numOutEdges);
//        System.out.println("Write: " + "Number edges: " + numOutEdges);
    for (IEdge<E, I, J> edge : outEdges) {
//            System.out.println("Write: " + "Edge:" + edge.getSinkVertexId() + " Class: " + edge.getSinkVertexId().getClass().getSimpleName());
      edge.getSinkVertexId().write(dataOutput);
      edge.getValue().write(dataOutput);
    }
//    System.out.println("Vertex ID Class,VertexValueClass:" + id.getClass() + "," + value.getClass());
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    throw new NotImplementedException("Use read fields with GiraphSubgraphConfiguration as parameter");
  }

  public void readFields(GiraphSubgraphConfiguration<?, I, V, E, ?, J> conf, DataInput dataInput) throws IOException {
    // Getting the subgraph vertex internals
    id = conf.createSubgraphVertexId();
    value = conf.createSubgraphVertexValue();

    id.readFields(dataInput);
    value.readFields(dataInput);
//    System.out.println("Read ID:" + id + "\t"+ id.getClass().getSimpleName());
//    System.out.println("Read: " + "Value:" + value + value.getClass().getSimpleName());

    int numEdges;
    numEdges = dataInput.readInt();
//    System.out.println("Read: " + "Number edges: " + numEdges);
    outEdges = Lists.newLinkedList();
    for (int i = 0; i < numEdges; i++) {
//      System.out.println("\n THIS IS I :  "+ i);
      DefaultSubgraphEdge<I, E, J> se = new DefaultSubgraphEdge<>();
      I targetId = conf.createSubgraphVertexId();
      E edgeValue = conf.createEdgeValue();
      targetId.readFields(dataInput);
      edgeValue.readFields(dataInput);
      se.initialize(null, edgeValue, targetId);
//      System.out.println("Read: " + "Edge:" + se.getSinkVertexId() + " Class: " + se.getSinkVertexId().getClass().getSimpleName());
      outEdges.add(se);
    }
  }


}
