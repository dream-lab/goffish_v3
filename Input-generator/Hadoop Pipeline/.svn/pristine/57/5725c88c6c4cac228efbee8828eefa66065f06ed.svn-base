package in.dream_lab.hadoopPipeline.cc;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/*
 * Job ID : 3
 * Job Name : PAL_to_CC_SPL
 * Job Description: Find subgraphs(WCC) within partitions	
 * Map Input File: PAL
 * Map Input Format :  V_id, [P_id, <E_id, V_sink>+]
 * Map Emit: P_id, [V_id, <E_id, V_sink>+]
 * Reducer Output File :
SPLAL		P_id, SG_id, V_id, <E_loc, V_loc>+
SPRAL		P_id, SG_id, V_id, <E_rem, V_rem>+
SPEL		P_id, SG_id, V_id, <isRemote, E_id, V_sink>+
SPVL		P_id, SG_id, V_id
  
 */
public class CCMapper extends Mapper <Object , Text, LongWritable, Text>{

	@Override
	protected void map(Object key, Text value,Context context)
			throws IOException, InterruptedException {
		
		String line=value.toString();
		
		String[] strs = line.split("\\s+");
		
		String[] vertexPartition =strs[0].split("#");
		
		//long vertexId=Long.parseLong(vertexPartition[0]);
    	long partitionId=Long.parseLong(vertexPartition[1]);
    	
    	StringBuilder sb =new StringBuilder();
    	sb.append(vertexPartition[0]).append("#").append(strs[1]);
    	
    	context.write( new LongWritable(partitionId),new Text(sb.toString()));
    	
	}

}
