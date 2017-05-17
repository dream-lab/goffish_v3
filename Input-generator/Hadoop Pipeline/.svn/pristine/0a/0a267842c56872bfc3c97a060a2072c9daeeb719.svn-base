package in.dream_lab.hadoopPipeline.cc;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Job ID : 2
 * Job Name : VP_AP_to_PAL
 * Job Description: Concatenate partition Id with vertex adjacency list
 * Map Input File: VP, EL
 * Map Input Format :V_id, [P_id]  V_src, [<E_id,V_sink>+]
 * Map Emit :V_id, [-1, P_id]    V_src, [V_sink, -1]
 * Reducer Emit: V_id, P_id, <E_id, V_sink>+
 * Reducer Output File :PAL
 * Note :Separator between P_id, <E_id, V_sink>+ is "#"
 * 
 */

public class ALFileMapper extends Mapper<Object , Text, LongWritable, Text> {

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String line=value.toString();
		
		String[] strs = line.trim().split("\\s+");
		long vertexId=Long.parseLong(strs[0]);
		String mapValue=strs[1]+"#E";
    	
		//System.out.println("AL: "+vertexId+ ":"+mapValue);
    	
    	context.write( new LongWritable(vertexId),new Text(mapValue));
    	
		
	}

	
}