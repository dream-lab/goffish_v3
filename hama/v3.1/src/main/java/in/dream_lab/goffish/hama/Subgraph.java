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

import java.util.*;

import com.google.common.collect.Iterables;
import org.apache.hadoop.io.Writable;

import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IRemoteVertex;
import in.dream_lab.goffish.api.ISubgraph;
import in.dream_lab.goffish.api.IVertex;

public class Subgraph<S extends Writable, V extends Writable, E extends Writable, I extends Writable, J extends Writable, K extends Writable>
    implements ISubgraph<S, V, E, I, J, K> {

  K subgraphID;
  private Map<I, IVertex<V, E, I, J>> _localVertexMap;
  private Map<I, IRemoteVertex<V, E, I, J, K>> _remoteVertexMap;
  int partitionID;
  S _value;

  public Subgraph(int partitionID, K subgraphID) {
    this.partitionID = partitionID;
    this.subgraphID = subgraphID;
    _localVertexMap = new HashMap<I, IVertex<V, E, I, J>>();
    _remoteVertexMap = new HashMap<I, IRemoteVertex<V, E, I, J, K>>();
  }

  public void addVertex(IVertex<V, E, I, J> v) {
    if (v instanceof IRemoteVertex)
      _remoteVertexMap.put(v.getVertexId(), (IRemoteVertex<V, E, I, J, K>) v);
    else
      _localVertexMap.put(v.getVertexId(), v);
  }

  @Override
  public IVertex<V, E, I, J> getVertexById(I vertexID) {
    return (_localVertexMap.get(vertexID) == null) ? _remoteVertexMap.get(vertexID) :
            _localVertexMap.get(vertexID);
  }

  @Override
  public K getSubgraphId() {
    return subgraphID;
  }

  @Override
  public long getVertexCount() {
    return _localVertexMap.size() + _remoteVertexMap.size();
  }

  @Override
  public long getLocalVertexCount() {
    return _localVertexMap.size();
  }

  @Override
  public long getRemoteVertexCount() {
    return _remoteVertexMap.size();
  }

  @Override
  public long getLocalEdgeCount() {
    long localEdge=0;
    for (IVertex<V, E, I, J> vertex : _localVertexMap.values()) {
      for(IEdge<E, I, J> temp : vertex.getOutEdges()) {
        localEdge++;
      }
    }
    return localEdge;
  }

  @Override
  public long getBoundaryEdgeCount() {
    long boundaryEdge=0;
    for (IVertex<V, E, I, J> localVertex : _localVertexMap.values()) {
      for(IEdge<E, I, J> temp : localVertex.getOutEdges()) {
        if(!_localVertexMap.containsKey(temp.getSinkVertexId()))
          boundaryEdge++;
      }
    }
return boundaryEdge;
  }

  @Override
  public long getBoundaryVertexCount() {
    long boundaryVertices=0;
    for (IVertex<V, E, I, J> localVertex : _localVertexMap.values()) {
      for(IEdge<E, I, J> temp : localVertex.getOutEdges()) {
        if(!_localVertexMap.containsKey(temp.getSinkVertexId())) {
          boundaryVertices++;
          break;
        }
      }
    }

    return boundaryVertices;
  }

  @Override
  public Iterable<IVertex<V, E, I, J>> getVertices() {
    return new Iterable<IVertex<V, E, I, J>>() {

      private Iterator<IVertex<V, E, I, J>> localVertexIterator = _localVertexMap.values().iterator();
      private Iterator<IRemoteVertex<V, E, I, J, K>> remoteVertexIterator = _remoteVertexMap.values().iterator();

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

  @Override
  public Iterable<IVertex<V, E, I, J>> getLocalVertices() {
    return _localVertexMap.values();
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
    return _remoteVertexMap.values();
  }

  @Override
  public IEdge<E, I, J> getEdgeById(J edgeID) {
    for (IVertex<V, E, I, J> vertex : _localVertexMap.values()) {
      for (IEdge<E, I, J> vertexEdge : vertex.getOutEdges()) {
        if (edgeID.equals(vertexEdge)) {
          return vertexEdge;
        }
      }
    }
    return null;
  }

  @Override
  public Iterable<IVertex<V,E,I,J>> getBoundaryVertices() {
   List <IVertex<V, E, I, J>> boundaryVertices = new ArrayList<>();
    for (IVertex<V, E, I, J> localVertex : _localVertexMap.values()) {
      for(IEdge<E, I, J> temp : localVertex.getOutEdges()) {
        if(!_localVertexMap.containsKey(temp.getSinkVertexId())) {
          boundaryVertices.add(localVertex);
          break;
        }
      }
    }
    return boundaryVertices;
  }

  @Override
  public Iterable<IEdge<E, I, J>> getBoundaryEdges() {
    List<IEdge<E,I,J>> boundaryEdge = new ArrayList<>();
    for (IVertex<V, E, I, J> localVertex : _localVertexMap.values()) {
      for(IEdge<E, I, J> temp : localVertex.getOutEdges()) {
        if(!_localVertexMap.containsKey(temp.getSinkVertexId()))
          boundaryEdge.add(temp);
      }
    }
    return boundaryEdge;
  }

  @Override
  public Iterable<IEdge<E, I, J>> getLocalOutEdges() {
    List<IEdge<E,I,J>> localEdge = new ArrayList<>();
    for (IVertex<V, E, I, J> vertex : _localVertexMap.values()) {
      for(IEdge<E, I, J> temp : vertex.getOutEdges()) {
        localEdge.add(temp);
      }
    }
    return localEdge;
  }


  @Override
  public Iterable<IEdge<E, I, J>> getOutEdges() {
    List<IEdge<E, I, J>> edgeList = new ArrayList<IEdge<E, I, J>>();
    for (IVertex<V, E, I, J> vertex : _localVertexMap.values()) {
      for (IEdge<E, I, J> vertexEdge : vertex.getOutEdges()) {
          edgeList.add(vertexEdge);
      }
    }
    return edgeList;
  }
}
