/*
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

package in.dream_lab.goffish.giraph.formats;

import in.dream_lab.goffish.api.ISubgraph;
import in.dream_lab.goffish.giraph.graph.SubgraphVertices;
import in.dream_lab.goffish.giraph.graph.SubgraphId;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Simple text based vertex output format example.
 */
public class SubgraphSimpleTextVertexOutputFormat extends
    TextVertexOutputFormat<SubgraphId<LongWritable>, SubgraphVertices, NullWritable> {
  /**
   * Simple text based vertex writer
   */
  private class SimpleTextVertexWriter extends TextVertexWriter {
    @Override
    public void writeVertex(
      Vertex<SubgraphId<LongWritable>, SubgraphVertices, NullWritable> vertex)
      throws IOException, InterruptedException {
      ISubgraph<LongWritable, NullWritable, NullWritable, LongWritable, NullWritable, LongWritable> subgraph = (ISubgraph) vertex;
      getRecordWriter().write(
          new Text(subgraph.getSubgraphId().toString()),
          new Text(subgraph.getSubgraphValue().toString()));
    }
  }

  @Override
  public TextVertexWriter createVertexWriter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    return new SimpleTextVertexWriter();
  }
}
