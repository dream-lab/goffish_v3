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

/* Defines remote vertex interface.
 * @param <V> Vertex value object type
 * @param <E> Edge value object type
 * @param <I> Vertex ID type
 * @param <J> Edge ID type
 * @param <K> Subgraph ID type
 * */
public interface IRemoteVertex<V extends Writable, E extends Writable, I extends Writable, J extends Writable, K extends Writable>  extends IVertex<V, E, I, J> {
  
  K getSubgraphId();
  
  void setLocalState(V value);
  
  V getLocalState();
}
