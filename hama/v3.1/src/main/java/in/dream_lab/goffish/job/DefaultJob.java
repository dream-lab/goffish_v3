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

package in.dream_lab.goffish.job;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import in.dream_lab.goffish.api.IVertex;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.InputFormat;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.OutputFormat;
import org.apache.hama.bsp.TextOutputFormat;

import in.dream_lab.goffish.api.AbstractSubgraphComputation;
import in.dream_lab.goffish.hama.GraphJob;
import in.dream_lab.goffish.hama.LongTextAdjacencyListReader;
import in.dream_lab.goffish.hama.NonSplitTextInputFormat;
import in.dream_lab.goffish.hama.api.IReader;

//TODO: Check null properties
public class DefaultJob {

  public static void main(String args[]) throws IOException,
      InterruptedException, ClassNotFoundException, ParseException {
    HamaConfiguration conf = new HamaConfiguration();
    
    Properties prop = getProperties(args[0]);
    String computeClass = prop.getProperty("computeClass");
    GraphJob job = new GraphJob(conf, (Class<? extends AbstractSubgraphComputation>)Class.forName(computeClass));
    job.setJobName("Default Job " + computeClass);

    String inputKeyClass = prop.getProperty("inputKeyClass");
    job.setInputKeyClass(Class.forName(inputKeyClass));
    
    String inputValueClass = prop.getProperty("inputValueClass");
    job.setInputValueClass(Class.forName(inputValueClass));

    String outputFormatClass = prop.getProperty("outputFormatClass");
    job.setOutputFormat((Class<? extends OutputFormat>)Class.forName(outputFormatClass));

    String outputKeyClass = prop.getProperty("outputKeyClass");
    job.setOutputKeyClass(Class.forName(outputKeyClass));

    String outputValueClass = prop.getProperty("outputValueClass");
    job.setOutputValueClass(Class.forName(outputValueClass));
    
    job.setInputPath(new Path(args[1]));
    job.setOutputPath(new Path(args[2]));
    
    String graphMessageClass = prop.getProperty("graphMessageClass");
    job.setGraphMessageClass((Class<? extends Writable>)Class.forName(graphMessageClass));

    // Reader configuration

    String inputFormatClass = prop.getProperty("inputFormatClass");
    if (inputFormatClass != null)
      job.setInputFormat((Class<? extends InputFormat>)Class.forName(inputFormatClass));

    String inputReaderClass = prop.getProperty("inputReaderClass");
    if (inputReaderClass != null)
      job.setInputReaderClass((Class<? extends IReader>)Class.forName(inputReaderClass));

    String vertexIDClass = prop.getProperty("vertexIDClass");
    if (vertexIDClass != null)
      job.setVertexIDClass(
          (Class<? extends Writable>) Class.forName(vertexIDClass));
    
    String edgeIDClass = prop.getProperty("edgeIDClass");
    if (edgeIDClass != null)
      job.setVertexIDClass(
          (Class<? extends Writable>) Class.forName(edgeIDClass));
    
    String subgraphIDClass = prop.getProperty("subgraphIDClass");
    if (subgraphIDClass != null)
      job.setSubgraphIDClass((Class<? extends Writable>) Class.forName(subgraphIDClass));

    String vertexClass = prop.getProperty("vertexClass");
    if (vertexClass != null)
      job.setVertexClass((Class<? extends IVertex>) Class.forName(vertexClass));
    
    String threadCount = prop.getProperty("threadCount");
    if (threadCount != null)
      job.setThreadCount(Integer.parseInt(threadCount));

    String initialInput = prop.getProperty("initialInput");
    if (initialInput != null)
      job.setInitialInput(initialInput);

    // Blocks till job completed
    job.waitForCompletion(true);
  }

  static Properties getProperties(String configFile) {
    Properties prop = new Properties();
    InputStream inputStream = null;

    try {
      inputStream = new FileInputStream(configFile);
      prop.load(inputStream);
      
    } catch (IOException ex) {
      ex.printStackTrace();
    } finally {
      if (inputStream != null) {
        try {
          inputStream.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    return prop;
  }
}
