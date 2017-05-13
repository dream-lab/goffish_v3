package in.dream_lab.hadoopPipeline.cc;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/*
 * Job ID : 4
 * Job Name : SPRAL_to_SPRSAL
 * Job Description: Append Subgraph & Partition ID to remote Adjacency  List in SPARAL file	
 * Map Input File: SPRAL
 * Map Input Format :  P_id, SG_id, V_id, <E_rem, V_rem>+
 * Map Emit: 
 * "for each Local vertex
			V_id, [P_id, SG_id]"		FLAG : 0
   "for each Remote vertex
			V_rem, [P_id, SG_id, V_id, <E_rem, V_rem>+]"  FLAG : 1
				
 * Reducer Output File : SPRSAL

	P_id, SG_id, V_id, <E_rem, V_rem, P_rem, SG_rem>+

*/

public class SPRALMapper extends Mapper<Object , Text, LongWritable, Text> {
	
	@Override
	protected void map(Object key, Text value,Context context)
			throws IOException, InterruptedException {
		// TODO parse the input 
		//Pid#SG	V#Vid:Eid,+++
		//String[] PidSGid =key.toString().split("#");
		//int Pid= Integer.parseInt(PidSGid[0]);
		//long SGid=Long.parseLong(PidSGid[1]); 
		
		String line=value.toString();
		
		String[] KeyValue = line.split("\\s+");
		
		String[] strs = KeyValue[1].split("#",-1);
		long V = Long.parseLong(strs[0]);
		
		//V_id, [P_id # SG_id]"
		//System.out.println("TEST1 :Key :"+V+ "  Value : "+"0:"+ KeyValue[0]);
		context.write(new LongWritable(V), new Text(new String("0:")+ KeyValue[0]));
		if(!strs[1].isEmpty()){
		String[] RemoteList =strs[1].split(",");//Note : It can be empty
		
		for(String edge : RemoteList){
			if(!edge.isEmpty()){
				//String[] VidEid = edge.split(":");
				long Vid=Long.parseLong(edge.split(":")[0]);
				//long eid=Long.parseLong(VidEid[1]);
				StringBuilder mapValue = new StringBuilder();
				mapValue.append("1:").append(KeyValue[0]).append("#").append(V).append(":").append(edge);
				context.write(new LongWritable(Vid), new Text(mapValue.toString()));
				//	V_rem, [1 : P_id# SG_id#V_id : <E_rem, V_rem>+]"  FLAG : 1
				//System.out.println("TEST2 :Key :"+Vid+ "  Value : "+mapValue);
			}
		}
	  }
	}	
}
