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

import java.io.IOException;

import in.dream_lab.goffish.api.IVertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.Combiner;
import org.apache.hama.bsp.Partitioner;
import org.apache.hama.bsp.BSPJob.JobState;
import org.apache.hama.bsp.PartitioningRunner.RecordConverter;
import org.apache.hama.bsp.message.MessageManager;
import org.apache.hama.bsp.message.OutgoingMessageManager;
import org.apache.hama.bsp.message.queue.MessageQueue;

import com.google.common.base.Preconditions;

import in.dream_lab.goffish.api.AbstractSubgraphComputation;
import in.dream_lab.goffish.api.ISubgraph;
import in.dream_lab.goffish.api.ISubgraphCompute;
import in.dream_lab.goffish.hama.api.IReader;

public class GraphJob extends BSPJob {

  public final static String SUBGRAPH_COMPUTE_CLASS_ATTR = "in.dream_lab.goffish.subgraphcompute.class";
  public final static String SUBGRAPH_CLASS_ATTR = "in.dream_lab.goffish.subgraph.class";
  public final static String GRAPH_MESSAGE_CLASS_ATTR = "in.dream_lab.goffish.message.class";
  public final static String VERTEX_ID_CLASS_ATTR = "in.dream_lab.goffish.vertexid.class";
  public final static String VERTEX_VALUE_CLASS_ATTR = "in.dream_lab.goffish.vertexvalue.class";
  public final static String EDGE_ID_CLASS_ATTR = "in.dream_lab.goffish.edgeid.class";
  public final static String EDGE_VALUE_CLASS_ATTR = "in.dream_lab.goffish.edgevalue.class";
  public final static String SUBGRAPH_ID_CLASS_ATTR = "in.dream_lab.goffish.subgraphid.class";
  public final static String SUBGRAPH_VALUE_CLASS_ATTR = "in.dream_lab.goffish.subgraphvalue.class";
  public final static String READER_CLASS_ATTR = "in.dream_lab.goffish.reader.class";
  public final static String VERTEX_CLASS_ATTR = "in.dream_lab.goffish.vertex.class";


  public final static String INITIAL_VALUE = "in.dream_lab.goffish.initialvalue";

  public GraphJob(HamaConfiguration conf,
      Class<? extends AbstractSubgraphComputation> exampleClass) throws IOException {

    super(conf);
    conf.setBoolean(Constants.ENABLE_RUNTIME_PARTITIONING, false);
    conf.setBoolean("hama.use.unsafeserialization", true);
    conf.setClass(SUBGRAPH_CLASS_ATTR, Subgraph.class, ISubgraph.class);
    conf.setClass(SUBGRAPH_COMPUTE_CLASS_ATTR, exampleClass, AbstractSubgraphComputation.class);

    this.setBspClass(GraphJobRunner.class);
    // Helps to determine the user's jar to distribute in the cluster.
    this.setJarByClass(exampleClass);
    // setting default values
    this.setVertexIDClass(LongWritable.class);
    this.setEdgeIDClass(LongWritable.class);
    this.setSubgraphIDClass(LongWritable.class);

    // this.setPartitioner(HashPartitioner.class);
  }

  @Override
  public void setPartitioner(
      @SuppressWarnings("rawtypes") Class<? extends Partitioner> theClass) {
    super.setPartitioner(theClass);
  }

  /**
   * Set the Vertex ID class for the job.
   */
  public void setVertexIDClass(Class<? extends Writable> cls)
      throws IllegalStateException {
    conf.setClass(VERTEX_ID_CLASS_ATTR, cls, Writable.class);
  }

  /**
   * Set the Vertex value class for the job.
   */
  public void setVertexValueClass(Class<? extends Writable> cls)
      throws IllegalStateException {
    conf.setClass(VERTEX_VALUE_CLASS_ATTR, cls, Writable.class);
  }

  /**
   * Set the Edge ID class for the job.
   */
  public void setEdgeIDClass(Class<? extends Writable> cls)
      throws IllegalStateException {
    conf.setClass(EDGE_ID_CLASS_ATTR, cls, Writable.class);
  }

  /**
   * Set the Edge value class for the job.
   */
  public void setEdgeValueClass(Class<? extends Writable> cls)
      throws IllegalStateException {
    conf.setClass(EDGE_VALUE_CLASS_ATTR, cls, Writable.class);
  }

  /**
   * Set the Subgraph ID class for the job.
   */
  public void setSubgraphIDClass(Class<? extends Writable> cls)
      throws IllegalStateException {
    conf.setClass(SUBGRAPH_ID_CLASS_ATTR, cls, Writable.class);
  }

  /**
   * Set the Subgraph value class for the job.
   */
  public void setSubgraphValueClass(Class<? extends Writable> cls)
      throws IllegalStateException {
    conf.setClass(SUBGRAPH_VALUE_CLASS_ATTR, cls, Writable.class);
  }

  /**
   * Set the Message Type for the Job.
   * 
   * @param cls
   */
  public void setGraphMessageClass(Class<? extends Writable> cls) {
    conf.setClass(GRAPH_MESSAGE_CLASS_ATTR, cls, Writable.class);
  }

  public void setVertexClass(Class<? extends IVertex> cls) {
    conf.setClass(VERTEX_CLASS_ATTR, cls, IVertex.class);
  }
  /**
   * Use RicherSubgraph Class instead of Subgraph for more features
   */
  public void useRicherSubgraph(boolean use) {
    if (use) {
      conf.setClass(SUBGRAPH_CLASS_ATTR, RicherSubgraph.class, ISubgraph.class);
    }
  }

  /**
   * Set the Subgraph class for the job.
   */
  // is this needed? can use exampleclass in constructor to do this
  public void setSubgraphComputeClass(
      Class<? extends SubgraphCompute<? extends Writable, ? extends Writable, ? extends Writable, ? extends Writable, ? extends Writable, ? extends Writable, ? extends Writable>> cls)
      throws IllegalStateException {
    conf.setClass(SUBGRAPH_COMPUTE_CLASS_ATTR, cls, ISubgraphCompute.class);
    setInputKeyClass(cls);
    setInputValueClass(NullWritable.class);
  }

  /**
   * Sets the input reader for parsing the input to vertices.
   */
  public void setInputReaderClass(
      @SuppressWarnings("rawtypes") Class<? extends IReader> cls) {
    conf.setClass(READER_CLASS_ATTR, cls, IReader.class);
  }

  /**
   * Sets the maximum number of supersteps for the application
   * 
   * @param maxIteration
   */
  public void setMaxIteration(int maxIteration) {
    conf.setInt("hama.graph.max.iteration", maxIteration);
  }

  /**
   * Sets the initial value that is passed to the constructor of the application
   * at runtime
   * 
   * @param input
   */
  public void setInitialInput(String input) {
    conf.set(INITIAL_VALUE, input);
  }

  @Override
  public void submit() throws IOException, InterruptedException {
    super.submit();
  }

}
