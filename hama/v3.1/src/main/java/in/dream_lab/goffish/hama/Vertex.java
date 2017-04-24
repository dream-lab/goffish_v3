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
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Writable;

import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IVertex;

public class Vertex<V extends Writable, E extends Writable, I extends Writable, J extends Writable>
    implements IVertex<V, E, I, J> {

  private List<IEdge<E, I, J>> _adjList;
  private I vertexID;
  private V _value;

  // Change to ArrayList for larger graphs (more memory efficient)
  Vertex() {
    _adjList = new LinkedList<IEdge<E, I, J>>();
  }

  Vertex(I ID) {
    this();
    vertexID = ID;
  }

  void addEdge(IEdge<E, I, J> edge) {
    _adjList.add(edge);
  }

  void setVertexID(I vertexID) {
    this.vertexID = vertexID;
  }

  @Override
  public I getVertexId() {
    return vertexID;
  }

  @Override
  public boolean isRemote() {
    return false;
  }

  @Override
  public Iterable<IEdge<E, I, J>> getOutEdges() {
    return _adjList;
  }

  @Override
  public V getValue() {
    return _value;
  }

  @Override
  public void setValue(V value) {
    _value = value;
  }

  @Override
  public IEdge<E, I, J> getEdgeByVertexId(I vertexID) {
    for (IEdge<E, I, J> e : _adjList)
      if (e.getSinkVertexId().equals(vertexID))
        return e;
    return null;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public boolean equals(Object o) {
    return (this.vertexID).equals(((IVertex) o).getVertexId());
  }
}
