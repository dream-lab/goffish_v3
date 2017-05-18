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

package in.dream_lab.goffish.hama.api;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.sync.SyncException;

import in.dream_lab.goffish.api.ISubgraph;

public interface IReader<KIn extends Writable, VIn extends Writable, KOut extends Writable, VOut extends Writable, S extends Writable, V extends Writable, E extends Writable, I extends Writable, J extends Writable, K extends Writable> {
  List<ISubgraph<S, V, E, I, J, K>> getSubgraphs()throws IOException, SyncException, InterruptedException ;
}
