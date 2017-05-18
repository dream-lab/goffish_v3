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

import org.apache.hadoop.io.Writable;

/*TODO: Add IWCSubgraph and ISCSubgraph
 * @param <S> Subgraph value object type
 * @param <V> Vertex value object type
 * @param <E> Edge value object type
 * @param <I> Vertex ID object type
 * @param <J> Edge ID object type
 * @param <K> Subgraph ID object type
 * */
public interface ISubgraph<S extends Writable, V extends Writable, E extends Writable, I extends Writable, J extends Writable, K extends Writable> {

  // TODO: add new class for IBiSubgraph
  // TODO: add edge efficient subgraph implementation also

  IVertex<V, E, I, J> getVertexById(I vertexID);

  K getSubgraphId();

  long getVertexCount();

  long getLocalVertexCount();

  Iterable<IVertex<V, E, I, J>> getVertices();

  Iterable<IVertex<V, E, I, J>> getLocalVertices();

  Iterable<IRemoteVertex<V, E, I, J, K>> getRemoteVertices();

  Iterable<IEdge<E, I, J>> getOutEdges();

  IEdge<E, I, J> getEdgeById(J edgeID);

  void setSubgraphValue(S value);

  S getSubgraphValue();
}
