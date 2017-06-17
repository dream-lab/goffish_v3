package in.dream_lab.hadoopPipeline.cc;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
/*
 * Job ID : 2
 * Job Name : VP_AP_to_PAL
 * Job Description: Concatenate partition Id with vertex adjacency list
 * Map Input File: VP, EL
 * Map Input Format :V_id, [P_id]  V_src, [<E_id,V_sink>+]
 * Map Emit :V_id, [-1, P_id]    V_src, [V_sink, -1]
 * Reducer Emit: V_id, P_id, <E_id, V_sink>+
 * Reducer Output File :PAL
 * Note :Separator between P_id, <E_id, V_sink>+ is ":"
 * Separator between Vid & Pid is '#'
 */
public class PALReducer extends Reducer< LongWritable, Text,Text,Text> {
	
	protected void reduce(LongWritable key, Iterable<Text> values , Context context)
			throws IOException, InterruptedException {
		
		
		String partitionId="";
		String adjList="";
		StringBuilder sb=new StringBuilder();
		for(Text value:values){
			
			//System.out.println("Reduce:" +key+":"+ value);
		   String line = value.toString();
		   
		   String[] strs = line.trim().split("#");
		   
		   //System.out.println("Reduce :" +strs[0]+ ":"+ strs[1]);
		   
		   if("P".equals(strs[0])){  /*This has partitionId*/
			   
			    partitionId =strs[1];
			 //   System.out.println("PartitionID "+partitionId);
			  
		   }
		   if("E".equals(strs[1])){ /*This has adjacency List*/
			   
			   adjList=strs[0];
			   //System.out.println("Adjlist "+adjList);
		   }
	}
//		System.out.println("Job 2 Edge Count: " + sb.toString() + "," + adjList);
		sb.append(key).append("#").append(partitionId);
		context.write(new Text(sb.toString()), new Text(adjList));
		
	}

}
