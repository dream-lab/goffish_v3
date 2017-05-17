package in.dream_lab.hadoopPipeline.cc;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Job ID : 5
 * Job Name : 
 * Job Description: Append Subgraph & Partition ID to remote Adjacency  	
 * Map Input File: SPRSAL

*/


//Sample input
//16#268435457#4763489    8300:3206562946180841472:25:419431105
//Pid#SGid#Vid	RVid#Eid#RPid#RSGid


public class SPRSALFileMapper extends Mapper<Object , Text , LongWritable , Text> {

	@Override
	protected void map(Object key, Text value,Context context)
			throws IOException, InterruptedException {
		// TODO parse the input
		//input P_id#SG_id#V_id, < V_rem : E_rem : P_rem : SG_rem>
		String line=value.toString();
		String[] KeyValue=line.split("\\s+");
		String inputValue=KeyValue[1].toString();
		String inputKey =KeyValue[0].toString();
		long V= Long.parseLong(inputKey.split("#")[2]);
		
		//USE flag 1 to indicate remote edge
		
		//System.out.println("TEST4 :"+inputValue);
		context.write(new LongWritable(V), new Text(new String("1!") +inputValue));
		
	}
	

}
