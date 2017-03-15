/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package in.dream_lab.goffish.hama;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Writable;

import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IRemoteVertex;
import in.dream_lab.goffish.api.IVertex;

/*
 * Extends Subgraph and has more Features that can be used for smaller graphs
 * without running out of memory.
 */
public class RicherSubgraph<S extends Writable, V extends Writable, E extends Writable, I extends Writable, J extends Writable, K extends Writable> extends Subgraph<S, V, E, I, J, K> {
  
  //question: use java.util map or hadoop map(is writable)?
  private Map<J, IEdge<E, I, J>> _edges;
  private List<IVertex<V, E, I, J>> _localVertices;
  private List<IRemoteVertex<V, E, I, J, K>> _remoteVertices;

  RicherSubgraph(int partitionID, K subgraphID) {
    super(partitionID, subgraphID);
    _edges = new HashMap<J, IEdge<E, I, J>>();
    _localVertices = new ArrayList<IVertex<V, E, I ,J>>();
    _remoteVertices = new ArrayList<IRemoteVertex<V, E, I, J, K>>();
  }
  
  @SuppressWarnings("unchecked")
  @Override
  void addVertex(IVertex<V, E, I, J> v) {
    if (v.isRemote()) {
      _remoteVertices.add((IRemoteVertex<V, E, I, J, K>)v);
    }
    else {
      _localVertices.add(v);
      for (IEdge<E, I, J> edge : v.getOutEdges()) {
        _edges.put(edge.getEdgeId(), edge);
      }
    }
  }
  
  @Override
  public long getLocalVertexCount() {
    return _localVertices.size();
  }
  
  @Override
  public Iterable<IVertex<V, E, I, J>> getLocalVertices() {
    return _localVertices;
  }
  
  @Override
  public Iterable<IRemoteVertex<V, E, I, J, K>> getRemoteVertices() {
    return _remoteVertices;
  }
  
  @Override
  public IEdge<E, I, J> getEdgeById(J edgeID) {
    return _edges.get(edgeID);
  }
  
  /*
   * Returns an iterable over all the edges of the subgraph
   */
  public Iterable<IEdge<E, I, J>> getOutEdges() {
    return _edges.values();
  }

}
