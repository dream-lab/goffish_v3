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
*/

package in.dream_lab.goffish.api;

import java.io.IOException;

import org.apache.hadoop.io.Writable;

/*
 * @param <S> Subgraph value object type
 * @param <V> Vertex value object type
 * @param <E> Edge value object type
 * @param <M> Message object type
 * @param <I> Vertex ID object type
 * @param <J> Edge ID object type
 * @param <K> Subgraph ID object type
 */
public interface ISubgraphCompute<S extends Writable, V extends Writable, E extends Writable, M extends Writable, I extends Writable, J extends Writable, K extends Writable> {

  ISubgraph<S, V, E, I, J, K> getSubgraph();// templatize return type, G extends
                                            // ISubgraph<S, V, E, I, J, K>

  void voteToHalt();
  
  boolean hasVotedToHalt();

  long getSuperstep();

  void sendMessageToSubgraph(K subgraphID, M message);

  void sendToVertex(I vertexID, M message);

  void sendToAll(M message); // auto fill subgraph ID on send or receive

  void sendToNeighbors(M message);

  void sendMessage(K subgraphID, Iterable<M> message);

  void sendToAll(Iterable<M> message);

  void sendToNeighbors(Iterable<M> message);

  String getConf(String key);

}
