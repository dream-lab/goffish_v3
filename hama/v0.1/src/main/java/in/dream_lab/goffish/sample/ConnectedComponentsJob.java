/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package in.dream_lab.goffish.sample;

import java.io.IOException;

import in.dream_lab.goffish.LongTextAdjacencyListReader;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.TextOutputFormat;

import in.dream_lab.goffish.GraphJob;
import in.dream_lab.goffish.NonSplitTextInputFormat;

public class ConnectedComponentsJob {

  public static void main(String args[]) throws IOException,
      InterruptedException, ClassNotFoundException, ParseException {
    HamaConfiguration conf = new HamaConfiguration();
    GraphJob job = new GraphJob(conf, ConnectedComponents.class);
    job.setJobName("Connected Components");

    job.setInputKeyClass(LongWritable.class);
    job.setInputValueClass(LongWritable.class);
    job.setOutputFormat(TextOutputFormat.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(LongWritable.class);
    job.setMaxIteration(2);
    job.setInputPath(new Path(args[0]));
    job.setOutputPath(new Path(args[1]));
    job.setGraphMessageClass(LongWritable.class);

    /* Reader configuration */
    job.setInputFormat(NonSplitTextInputFormat.class);
    job.setInputReaderClass(LongTextAdjacencyListReader.class);

    // Blocks till job completed
    job.waitForCompletion(true);
  }
}
