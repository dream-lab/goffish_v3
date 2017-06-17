package in.dream_lab.hadoopPipeline.cc;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.JobID;
/*
 * @uthor:RavikantDindokar
 * Job ID : 1
 * Job Name : EL_to_UEL
 * Job Description: Convert directed to undirected graph
 * Map Input File: EL (SNAP file)
 * Map Input Format :V_src, V_sink
 * Map Emit :V_src, [V_sink]  && V_sink, [V_src]
 * Reducer Emit: V_src, E_id:V_sink && V_sink, E_id:V_src
 * Reducer Output File :UEL
 * Note :Remove duplicates
 * EdgeId : first  8 bits : partitionId, next  40bits : local count , last 16 bits : left for future use(instance data)
 */

public class MakeGraphUndirectedReducer extends Reducer< LongWritable, LongWritable,LongWritable,Text> {

	public static long localcount= 0L;
	@Override
	protected void reduce(LongWritable key, Iterable<LongWritable> values , Context context)
			throws IOException, InterruptedException {
		
		
		/*final String jobId = System.getenv("mapred_job_id");
		String[] strs=jobId.split("_");
		
		String reducerStr =strs[strs.length - 1];
		long reducerId=Long.parseLong(reducerStr);
		
		//System.out.println("TEST : TASKId :"+context.getTaskAttemptID().getTaskID().getId()+ " reducerID :"+reducerStr);
		*/
		LongWritable sourceId=key;
		Set<Long> adjlist = new HashSet<Long>();
//		long i = 0;
		for(LongWritable v : values){
			adjlist.add(v.get());
//			i++;
//			if ((i % 1000) == 0) {
//				System.out.println("Processed " + i + "values");
//			}
		}
		
//		System.out.println("Job 1 Edge count: " + sourceId.get() + "," + adjlist.size());
				
		long taskId=context.getTaskAttemptID().getTaskID().getId();
		
		 long edgeId;
		
		String edgeSinkPair="";
		
		StringBuilder sb =new StringBuilder();
		
		String prefix = "";
		
		for(Long sinkId : adjlist){
			
			localcount++;
			
			edgeId=(localcount <<16)|(taskId << 55);
			
			 sb.append(prefix);
			 
			 prefix = ",";
			
			 edgeSinkPair= edgeId +":"+sinkId;
			
			sb.append(edgeSinkPair);
			
		}
	
		context.write(sourceId, new Text(sb.toString()));
	}

}
