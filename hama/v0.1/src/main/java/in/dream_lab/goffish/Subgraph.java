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
package in.dream_lab.goffish;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.Writable;

import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IRemoteVertex;
import in.dream_lab.goffish.api.ISubgraph;
import in.dream_lab.goffish.api.IVertex;

public class Subgraph<S extends Writable, V extends Writable, E extends Writable, I extends Writable, J extends Writable, K extends Writable>
    implements ISubgraph<S, V, E, I, J, K> {
  K subgraphID;
  private Map<I, IVertex<V, E, I, J>> _vertexMap;
  int partitionID;
  S _value;

  Subgraph(int partitionID, K subgraphID) {
    this.partitionID = partitionID;
    this.subgraphID = subgraphID;
    _vertexMap = new HashMap<I, IVertex<V, E, I, J>>();
  }

  void addVertex(IVertex<V, E, I, J> v) {
    _vertexMap.put(v.getVertexId(), v);
  }

  @Override
  public IVertex<V, E, I, J> getVertexById(I vertexID) {
    return _vertexMap.get(vertexID);
  }

  @Override
  public K getSubgraphId() {
    return subgraphID;
  }

  @Override
  public long getVertexCount() {
    return _vertexMap.size();
  }

  @Override
  public long getLocalVertexCount() {
    long localVertexCount = 0;
    for (IVertex<V, E, I, J> v : _vertexMap.values())
      if (!v.isRemote())
        localVertexCount++;
    return localVertexCount;
  }

  @Override
  public Iterable<IVertex<V, E, I, J>> getVertices() {
    return _vertexMap.values();
  }

  /* Avoid using this function as it is inefficient. */
  @Override
  public Iterable<IVertex<V, E, I, J>> getLocalVertices() {
    List<IVertex<V, E, I, J>> localVertices = new ArrayList<IVertex<V, E, I, J>>();
    for (IVertex<V, E, I, J> v : _vertexMap.values())
      if (!v.isRemote())
        localVertices.add(v);
    return localVertices;
  }

  @Override
  public void setSubgraphValue(S value) {
    _value = value;
  }

  @Override
  public S getSubgraphValue() {
    return _value;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Iterable<IRemoteVertex<V, E, I, J, K>> getRemoteVertices() {
    List<IRemoteVertex<V, E, I, J, K>> remoteVertices = new ArrayList<IRemoteVertex<V, E, I, J, K>>();
    for (IVertex<V, E, I, J> v : _vertexMap.values())
      if (v.isRemote())
        remoteVertices.add((IRemoteVertex<V, E, I, J, K>) v);
    return remoteVertices;
  }

  @Override
  public IEdge<E, I, J> getEdgeById(J edgeID) {
    for (IVertex<V, E, I, J> vertex : _vertexMap.values()) {
      for (IEdge<E, I, J> vertexEdge : vertex.getOutEdges()) {
        if (edgeID.equals(vertexEdge)) {
          return vertexEdge;
        }
      }
    }
    return null;
  }

  @Override
  public Iterable<IEdge<E, I, J>> getOutEdges() {

    List<IEdge<E, I, J>> edgeList = new ArrayList<IEdge<E, I, J>>();

    for (IVertex<V, E, I, J> vertex : _vertexMap.values()) {
      if (vertex.isRemote())
        continue;
      for (IEdge<E, I, J> vertexEdge : vertex.getOutEdges()) {
        edgeList.add(vertexEdge);
      }
    }
    return edgeList;
  }
}
