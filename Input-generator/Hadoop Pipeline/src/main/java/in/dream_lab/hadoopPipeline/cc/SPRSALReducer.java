package in.dream_lab.hadoopPipeline.cc;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
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
				
 * Reducer Output File : SPRSAL(Key,Value)

	P_id#SG_id#V_id  E_rem:V_rem:P_rem:SG_rem

*/
public class SPRSALReducer extends Reducer<LongWritable, Text, Text, Text> {

	@Override
	protected void reduce(LongWritable key, Iterable<Text> values , Context context)
			throws IOException, InterruptedException {
		// TODO Parse the input
		int RPid=0;
		long Rsgid=0L;
//		int entry_count=0;
                List<String> cachedValues = new ArrayList<String>();
		for(Text v : values){
			//System.out.println("TEST3: "+v);
			//check for FLAG
			cachedValues.add(v.toString());
//			entry_count++;
			String[] splitVal =v.toString().split(":");
			String flag = splitVal[0];
//                        System.out.println(v.toString());
			//System.out.println(" TEST3: "+flag);
			if(flag.equals("0")){ //FLAG : 0  Local vertex 
				//V_id, [P_id # SG_id]
				//System.out.println("TEST4:"+splitVal[0]+" &&  "+splitVal[1]);
				 RPid =Integer.parseInt(splitVal[1].split("#")[0]);
				 Rsgid =Long.parseLong(splitVal[1].split("#")[1]);
				//break;
			}
		}
//                long countr = 0L;
//		System.out.println("entryCount "+entry_count+" "+cachedValues.size() + " Key: " + key.get());
		for(String v : cachedValues){
//                        countr++;
			//check for FLAG
			String[] splitVal =v.toString().split(":");
			String flag = splitVal[0];
			if(flag.equals("1")){  //FLAG : 1  Remote vertex
				//V_rem, [1 : P_id# SG_id#V_id : <E_rem, V_rem>+]"  FLAG : 1
//				System.out.println("TEST3: Key: " + key.get() + " Value: " + v.toString());
				StringBuilder reduceValue=  new StringBuilder();
				reduceValue.append(splitVal[2]).append(":").append(splitVal[3]).append(":").append(RPid).append(":").append(Rsgid);
				context.write(new Text(splitVal[1]), new Text(reduceValue.toString()));
				
			}
			
			
		}
//		System.out.println("Remote Edge Count: " + countr + " " + "Key: " + key.get());
		
	}

	
}
