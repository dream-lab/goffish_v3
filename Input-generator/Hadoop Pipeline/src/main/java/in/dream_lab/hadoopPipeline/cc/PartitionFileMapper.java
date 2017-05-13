package in.dream_lab.hadoopPipeline.cc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* @uthor:Ravikant
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

public class PartitionFileMapper extends Mapper<Object , Text, LongWritable, Text> {

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		
		Configuration conf = context.getConfiguration();
    	String partition_count = conf.get("partition_count");
    	
    	//Long subtitute_for_partition0=Long.parseLong(partition_count);
    	
		String line=value.toString();
		
		String[] strs = line.trim().split("\\s+");
		long vertexId=Long.parseLong(strs[0]);
    	long partitionId=Long.parseLong(strs[1]);
    	
    	String mapValue;
    	
    	//if(partitionId == 0L){
    	//	 mapValue="P#"+subtitute_for_partition0;
    	//}else{
    	 mapValue="P#"+partitionId;
    	//}
    	//System.out.println("PART: "+strs[0]+":"+strs[1]+":"+vertexId+ ":"+mapValue);
    	
    	context.write( new LongWritable(vertexId),new Text(mapValue));
    	
		
	}

	
}
