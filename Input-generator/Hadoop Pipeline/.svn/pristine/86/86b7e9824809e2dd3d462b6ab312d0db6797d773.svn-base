package in.dream_lab.hadoopPipeline.cc;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Job ID : 5
 * Job Name : 
 * Job Description: Append Subgraph & Partition ID to remote Adjacency  	
 * Map Input File: SPLALFileMapper
 * INput FIle Format:
 * PID#SG#VID				RemoteV#RemoteE#RemotePID#RemoteSGID
 * 19#318767370#4639       2667:684547143449247744:0:1
 * 	
	
*/

public class SPLALFileMapper extends Mapper<Object , Text , LongWritable , Text> {

	@Override
	protected void map(Object key, Text value,Context context)
			throws IOException, InterruptedException {
		// TODO parse the input
		//P_id#SG_id, V_id#<V_loc :E_loc >+ comma separated
		//System.out.println("TEST1: "+value);
		String input = value.toString();
		String[] KeyValue =input.split("\\s+");
		//emit V_id, [P_id, SG_id, <E_loc, V_loc>+] with flag 0
		String[] inputValue=KeyValue[1].split("#",-1);
		long V=Long.parseLong(inputValue[0]);
		StringBuilder outputValue= new StringBuilder();
		outputValue.append(KeyValue[0]).append("@");
		if(!inputValue[1].isEmpty()){
		String[] localEdges=inputValue[1].split(",",-1);
		for(String edge : localEdges){
			if(!edge.isEmpty())
			outputValue.append(edge).append(",");
		}}
		//System.out.println("TEST2: "+outputValue);
		context.write(new LongWritable(V), new Text(new String("0!") +outputValue.toString()));
		
	}

}
