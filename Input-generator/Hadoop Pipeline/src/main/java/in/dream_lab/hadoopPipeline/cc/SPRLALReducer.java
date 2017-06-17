package in.dream_lab.hadoopPipeline.cc;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/*
 * Job ID : 5
 * Job Name : 
 * Job Description: Append Subgraph & Partition ID to remote Adjacency  	
 *

*/

//emit  from SPLALFileMapper  V_id 0!P_id#SG_id $ <E_loc, V_loc>,+ 
//emit from SPRSALFileMapper  V_id	1!V_rem : E_rem : P_rem : SG_rem
public class SPRLALReducer extends Reducer< LongWritable, Text,Text,Text>  {

	@Override
	protected void reduce(LongWritable key, Iterable<Text> values,
			Context context)
			throws IOException, InterruptedException {
		// TODO parse the input based on flag
		//StringBuilder reduceKey = new StringBuilder();
		//StringBuilder reduceValue = new StringBuilder();
		StringBuilder localEdges = new StringBuilder();
		StringBuilder remoteEdges = new StringBuilder();
		//int PartitionID, subgraphID;
		String PartitionSubgraphID="";
		String PartitionID = "";
		String SubgraphID = "";
//                long counts = 0L;
//                long countl = 0L;
//                long countr = 0L;
		// input SPRSALFileMapper V 1*V_rem : E_rem : P_rem : SG_rem
		for(Text v: values){
			String mapValue=v.toString();
			String[] SplitMapValue = mapValue.split("!");
			if(SplitMapValue[0].equals("1")){//RemoteEdge
				
				//emit from SPRSALFileMapper  V_id	1!V_rem : E_rem : P_rem : SG_rem			
				
				//System.out.println("TESTR1: "+mapValue);
			        
			        String inputValueArr[] = SplitMapValue[1].split(":");
		                //RVid RSGid RPid
		                String outputValue = inputValueArr[0] + " " + inputValueArr[3] + " " + inputValueArr[2];
				
				remoteEdges.append(" ").append(outputValue);
//                                countr++;
			}else{//Local edges
				
				//emit  from SPLALFileMapper  P_id V_id SG_id <V_loc SG_id P_id>,+
				
				//System.out.println("TESTR2: "+mapValue);
				
				//P_id#SG_id$ <E_loc, V_loc>+],.....
				String[] LocalEdgeRecord=SplitMapValue[1].split("@",-1);
				PartitionSubgraphID=LocalEdgeRecord[0];
				PartitionID = PartitionSubgraphID.split("#")[0];
				SubgraphID = PartitionSubgraphID.split("#")[1];
//				System.out.println("Subgraph ID:" + SubgraphID);
				if(!LocalEdgeRecord[1].isEmpty()){
				for(String edge: LocalEdgeRecord[1].split(",")){
					if(!edge.isEmpty()){
					  //edge in format sinkID:edgeID
					  String SinkID = edge.split(":")[0];
					  
					localEdges.append(" ").append(SinkID).append(" ").append(SubgraphID).append(" ").append(PartitionID); }
//              				countl++;
					
				}
			}
			}
		}
//		System.out.println("Remote Edge Count:" + countr);
//                System.out.println("Local Edge Count:" + countl);
		//local and remote edge objects have a preceding space so we dont need to give extra spaces
		context.write(new Text(PartitionID), new Text(key.toString()+" "+SubgraphID+localEdges.toString()+remoteEdges.toString()));
		
	}

}
