package in.dream_lab.hadoopPipeline.cc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


/*@author:Ravikant
 * Job ID : 1
 * Job Name : EL_to_UEL
 * Job Description: Convert directed to undirected graph
 * Map Input File: EL (SNAP file)
 * Map Input Format :V_src, V_sink
 * Map Emit :V_src, [V_sink]  && V_sink, [V_src]
 * Reducer Emit: V_src, E_id:V_sink && V_sink, E_id:V_src
 * Reducer Output File :UEL
 * Note :Remove duplicates
 * 
 */

public class MakeGraphUndirectedMapper extends Mapper<Object , Text, LongWritable, LongWritable> {

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		
		String line=value.toString();
		if(!line.startsWith("#")){
		String[] strs = line.trim().split("\\s+");
		long sinkId=Long.parseLong(strs[1]);
    	long sourceId=Long.parseLong(strs[0]);
    	
    	context.write( new LongWritable(sourceId),new LongWritable(sinkId));
    	/*
    	 * Comment the following line to make it directed
    	 */
    	
    	Configuration conf = context.getConfiguration();
    	String param = conf.get("makeUndirected");
    	if(Integer.parseInt(param)==1){
    		//System.out.println("TEST: makeUndirected is called");
    	context.write(new LongWritable(sinkId), new LongWritable(sourceId));
    	}
		}
	}

}
