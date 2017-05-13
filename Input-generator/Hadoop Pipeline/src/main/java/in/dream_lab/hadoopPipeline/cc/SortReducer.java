package in.dream_lab.hadoopPipeline.cc;

import in.dream_lab.hadoopPipeline.cc.IEdge;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/* @uthor:RavikantDindokar
 * Job ID : 3
 * Job Name : PAL_to_CC_SPL
 * Job Description: Find subgraphs(WCC) within partitions	
 * Map Input File: PAL
 * Map Input Format :  V_id, [P_id, <E_id, V_sink>+]
 * Map Emit :Pid [  V_src,#[Eid,V_sink ]
 * 
 * Reducer Output File :
SPLAL		P_id, SG_id, V_id, <E_loc, V_loc>+
SPRAL		P_id, SG_id, V_id, <E_rem, V_rem>+
SPEL		P_id, SG_id, V_id, <isRemote, E_id, V_sink>+
SPVL		P_id, SG_id, V_id
  
 */
public class SortReducer extends Reducer< LongWritable, Text,Text,Text> {

	@Override
	protected void reduce(LongWritable key, Iterable<Text> values , Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	  //key = pid
	  //value = pid sgid vid sinkid1 sinksgid1 sinkpid1 .......
		for(Text value: values){
		        String outVal = value.toString().substring(value.toString().indexOf('\t')+1);
			context.write(new Text(""+key.get()), new Text(outVal));
		}
		
	}

}
