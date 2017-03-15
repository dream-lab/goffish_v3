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

import in.dream_lab.goffish.api.IPartition;
import in.dream_lab.goffish.api.ISubgraph;

public class Partition<S extends Writable, V extends Writable, E extends Writable, I extends Writable, J extends Writable, K extends Writable>
    implements IPartition<S, V, E, I, J, K> {

  private int partitionID;
  private List<ISubgraph<S, V, E, I, J, K>> _subgraphs;
  private Map<K, ISubgraph<S, V, E, I, J, K>> _subgraphMap;

  Partition(int ID) {
    partitionID = ID;
    _subgraphs = new ArrayList<ISubgraph<S, V, E, I, J, K>>();
    _subgraphMap = new HashMap<K, ISubgraph<S, V, E, I, J, K>>();
  }

  @Override
  public int getPartitionId() {
    return partitionID;
  }

  public void addSubgraph(ISubgraph<S, V, E, I, J, K> subgraph) {
    _subgraphs.add(subgraph);
    _subgraphMap.put(subgraph.getSubgraphId(), subgraph);
  }

  public List<ISubgraph<S, V, E, I, J, K>> getSubgraphs() {
    return _subgraphs;
  }

  @Override
  public void removeSubgraph(K subgraphID) {
    _subgraphs.remove(_subgraphMap.get(subgraphID));
    _subgraphMap.remove(subgraphID);
  }

  @Override
  public ISubgraph<S, V, E, I, J, K> getSubgraph(K subgraphID) {
    return _subgraphMap.get(subgraphID);
  }
}
