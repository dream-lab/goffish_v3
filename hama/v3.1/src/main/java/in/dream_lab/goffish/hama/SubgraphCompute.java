/**
 *  Copyright 2017 DREAM:Lab, Indian Institute of Science, Bangalore
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may
 *  not use this file except in compliance with the License. You may obtain
 *  a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  
 *  @author Himanshu Sharma
 *  @author Diptanshu Kakwani
*/

package in.dream_lab.goffish.hama;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.io.Writable;

import in.dream_lab.goffish.api.AbstractSubgraphComputation;
import in.dream_lab.goffish.api.IMessage;
import in.dream_lab.goffish.api.ISubgraph;
import in.dream_lab.goffish.api.ISubgraphCompute;

public class SubgraphCompute<S extends Writable, V extends Writable, E extends Writable, M extends Writable, I extends Writable, J extends Writable, K extends Writable>
    implements ISubgraphCompute<S, V, E, M, I, J, K> {

  private ISubgraph<S, V, E, I, J, K> subgraph;
  private AbstractSubgraphComputation<S, V, E, M, I, J, K> abstractSubgraphCompute;
  long superStepCount;
  boolean voteToHalt;
  private GraphJobRunner<S, V, E, M, I, J, K> runner;

  public void init(GraphJobRunner<S, V, E, M, I, J, K> runner) {
    this.runner = runner;
    this.voteToHalt = false;
  }

  @Override
  public long getSuperstep() {
    return runner.getSuperStepCount();
  }

  @Override
  public ISubgraph<S, V, E, I, J, K> getSubgraph() {
    return subgraph;
  }

  @Override
  public void voteToHalt() {
    voteToHalt = true;
  }

  @Override
  public boolean hasVotedToHalt() {
    return voteToHalt;
  }

  /*
   * Resumes the subgraph from halted state
   */
  void setActive() {
    this.voteToHalt = false;
  }

  void setSubgraph(ISubgraph<S, V, E, I, J, K> subgraph) {
    this.subgraph = subgraph;
  }

  @Override
  public void sendMessage(K subgraphID, Iterable<M> messages) {
    for (M message : messages) {
      this.sendMessageToSubgraph(subgraphID, message);
    }
  }

  @Override
  public void sendMessageToSubgraph(K subgraphID, M message) {
    runner.sendMessage(subgraph.getSubgraphId(), subgraphID, message);
  }

  @Override
  public void sendToVertex(I vertexID, M message) {
    runner.sendToVertex(subgraph.getSubgraphId(), vertexID, message);
  }

  @Override
  public void sendToAll(Iterable<M> messages) {
    for (M message : messages) {
      this.sendToAll(message);
    }
  }

  @Override
  public void sendToAll(M message) {
    runner.sendToAll(subgraph.getSubgraphId(), message);
  }

  @Override
  public void sendToNeighbors(Iterable<M> messages) {
    for (M message : messages) {
      this.sendToNeighbors(message);
    }
  }

  @Override
  public void sendToNeighbors(M message) {
    runner.sendToNeighbors(subgraph, message);
  }

  public void setAbstractSubgraphCompute(
      AbstractSubgraphComputation<S, V, E, M, I, J, K> comp) {
    this.abstractSubgraphCompute = comp;
  }
  
  public AbstractSubgraphComputation<S, V, E, M, I, J, K> getAbstractSubgraphCompute() {
    return this.abstractSubgraphCompute;
  }

  public void compute(Iterable<IMessage<K, M>> messages) throws IOException {
    abstractSubgraphCompute.compute(messages);
  }

  @Override
  public String getConf(String key) {
    // TODO Auto-generated method stub
    return null;
  }

}
