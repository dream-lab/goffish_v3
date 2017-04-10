package in.dream_lab.goffish.giraph.graph;

import in.dream_lab.goffish.api.*;
import org.apache.giraph.conf.ClassConfOption;
import org.apache.giraph.graph.*;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.hadoop.io.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by anirudh on 27/09/16.
 *
 * @param <S>  Subgraph id
 * @param <I>  Vertex id
 * @param <V>  Vertex value
 * @param <E>  Edge data
 * @param <M>  Message type
 * @param <SV> Subgraph Value type
 */


// S subgraph value type ----- SV now
// V vertex object type   -- V is the vertex value
// E edge value type    -- E is the edge value how
// M msg object type    -- M is the message value type
// I vertex id      --- I is the vertex id here
// J edge id        -- EI
// K subgraph id  ---- S

public class GiraphSubgraphComputation<S extends WritableComparable,
    I extends WritableComparable, V extends WritableComparable, E extends Writable, M extends Writable, SV extends Writable, EI extends WritableComparable> extends BasicComputation<SubgraphId<S>, SubgraphVertices<SV, V, E, I, EI, S>, E, SubgraphMessage<S,M>>
    implements ISubgraphCompute<SV, V, E, M, I, EI, S> {

  private static final Logger LOG = Logger.getLogger(GiraphSubgraphComputation.class);

  private static final ClassConfOption<AbstractSubgraphComputation> SUBGRAPH_COMPUTATION_CLASS = ClassConfOption.create("subgraphComputationClass",
      null, AbstractSubgraphComputation.class, "Subgraph Computation Class");


  private AbstractSubgraphComputation<SV, V, E, M, I, EI, S> abstractSubgraphComputation;

  // TODO: Have class be specified in conf

  @Override
  public void sendToVertex(I vertexID, M message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void sendToAll(M message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void sendMessage(S subgraphId, Iterable<M> messages) {
    for (M message: messages) {
      sendMessageToSubgraph(subgraphId, message);
    }
  }

  @Override
  public void sendToAll(Iterable<M> messages) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void sendToNeighbors(Iterable<M> messages) {
    for (M message : messages) {
      sendToNeighbors(message);
    }
  }

  private DefaultSubgraph<SV, V, E, I, EI, S> subgraph;

  public ISubgraph<SV, V, E, I, EI, S> getSubgraph() {
    return subgraph;
  }
  // TODO: Take care of state changes for the subgraph passed

  public void compute(Vertex<SubgraphId<S>, SubgraphVertices<SV, V, E, I, EI, S>, E> vertex, Iterable<SubgraphMessage<S,M>> messages) throws IOException {
    Class userSubgraphComputationClass;
    long superstep = super.getSuperstep();
    if (abstractSubgraphComputation == null) {
      if (superstep == 0) {
        userSubgraphComputationClass = RemoteVerticesFinder.class;
      } else if (superstep == 1) {
        userSubgraphComputationClass = RemoteVerticesFinder2.class;
      } else if (superstep == 2) {
        userSubgraphComputationClass = RemoteVerticesFinder3.class;
      } else {
        userSubgraphComputationClass = SUBGRAPH_COMPUTATION_CLASS.get(getConf());
        LOG.info("User Class: " + userSubgraphComputationClass);
      }
      abstractSubgraphComputation = (AbstractSubgraphComputation<SV, V, E, M, I, EI, S>) ReflectionUtils.newInstance(userSubgraphComputationClass, getConf());
    }
    abstractSubgraphComputation.setSubgraphPlatformCompute(this);
    subgraph = (DefaultSubgraph) vertex;
    abstractSubgraphComputation.compute(new IMessageIterable(messages));
  }

  public void sendToNeighbors(M message) {
    WritableComparable subgraphId = subgraph.getSubgraphId();
    SubgraphMessage sm = new SubgraphMessage(subgraphId, message);
    super.sendMessageToAllEdges(subgraph, sm);
  }

  public void sendToNeighbors(DefaultSubgraph<SV, V, E, I, EI, S> subgraph, M message) {
    WritableComparable subgraphId = subgraph.getSubgraphId();
    SubgraphMessage sm = new SubgraphMessage(subgraphId, message);
    super.sendMessageToAllEdges(subgraph, sm);
  }


  private int getPartition(S subgraphId) {
    return ((IntWritable) subgraph.getSubgraphVertices().getSubgraphParitionMapping().get(subgraphId)).get();
  }

  public void voteToHalt() {
    subgraph.voteToHalt();
  }

  IEdge<E, I, EI> getEdgeById(EI id) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getSuperstep() {
    return super.getSuperstep() - 3;
  }

  @Override
  public boolean hasVotedToHalt() {
    return subgraph.isHalted();
  }

  @Override
  public void sendMessageToSubgraph(S subgraphId, M message) {
    SubgraphMessage sm = new SubgraphMessage(subgraphId, message);
    SubgraphId<S> sId = new SubgraphId<>(subgraphId, getPartition(subgraphId));
    sendMessage(sId, sm);
  }

  @Override
  public String getConf(String key) {
    return getConf().get(key);
  }

  private class IMessageIterable implements Iterable<IMessage<S,M>> {
    private Iterable<SubgraphMessage<S, M>> subgraphMessagesIterable;

    public IMessageIterable(Iterable<SubgraphMessage<S, M>> subgraphMessagesIterable) {
      this.subgraphMessagesIterable = subgraphMessagesIterable;
    }


    @Override
    public Iterator<IMessage<S, M>> iterator() {
      final Iterator<SubgraphMessage<S, M>> subgraphMessagesIterator = subgraphMessagesIterable.iterator();
      return new Iterator<IMessage<S, M>>() {
        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasNext() {
          return subgraphMessagesIterator.hasNext();
        }

        @Override
        public IMessage<S, M> next() {
          return subgraphMessagesIterator.next();
        }
      };
    }
  }
}
