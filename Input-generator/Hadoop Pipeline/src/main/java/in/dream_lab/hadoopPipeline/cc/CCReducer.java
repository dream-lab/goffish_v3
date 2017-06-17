package in.dream_lab.hadoopPipeline.cc;

import in.dream_lab.hadoopPipeline.cc.IEdge;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.*;

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
public class CCReducer extends Reducer< LongWritable, Text,Text,Text> {

	private MultipleOutputs<Text, Text> multipleOutputs;
	
	@Override
	protected void setup(Context context)throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		multipleOutputs = new MultipleOutputs<Text, Text>(context);
	}
	
	@Override
	protected void reduce(LongWritable key, Iterable<Text> values , Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		/*Make graph undirected 
		 * Adjacency list in the form <Long , Set<LONG> >
		 * Maintain remote vertices
		 * Maintain Local edges
		 * 	input for ConnectedComponents:  HashMap <Long, Set<Long>> 	
		*/
		Configuration conf = context.getConfiguration();
    	String graphID = conf.get("GraphID");
		
		//System.out.println("TEST: REDUCER called for partitionID "+key);
		Set<Long> LocalVertex =new TreeSet<Long>();  // TODO: camelcase var name // TODO: base type to be used for var. e.g. Set, Map
		Map <Long, HashMap<Long,IEdge>> AdjMatrix= new HashMap<Long, HashMap<Long,IEdge>>(); // TODO: camelcase var name // TODO: Map<Long, Map<SinkId, EdgeIsRemotePair>>; //TODO: default value for isRemote is false.
		
		//HashMap<Long,TreeSet<Long>> LocalAdjMatrix = new HashMap<Long, TreeSet<Long>>(); // TODO: remove. Overloaded by adjMat's isRemote field
		long count=0L;
		long sourceID;
		//Parse the data from input file as Adjacency Matrix
		for(Text value: values){
			count++;
			
			String str=value.toString();
			String[] strs=str.split("#");// TODO: why trim?
			sourceID=Long.parseLong(strs[0]);
			//sourceVertex.add(sourceID);
			
			//System.out.println("TEST: partitionID "+key+ " sourceV "+sourceID);
			
			String[] edgeSink=strs[1].split(",");
			HashMap<Long,IEdge> EdgeList = new HashMap<Long, IEdge>(); // TODO: List<EdgeSinkPair>
			for(String e : edgeSink){
				
				String[] strs1=e.split(":");
				long edgeID=Long.parseLong(strs1[0]);
				long sinkID=Long.parseLong(strs1[1]);
				IEdge e1 = new IEdge();
				e1.createEdge(edgeID);
				EdgeList.put(sinkID, e1);  //Note : Key is sinkVertex ID 
				//AdjacencyMatrix.put(sourceID, EdgeList); // FIXME: 
			}
			
			AdjMatrix.put(sourceID, EdgeList);
		} // FINISHED PARSING FULL INPUT TO REDUCER
		//System.out.println("TEST:  partitionID "+key+ " vertexCount "+count);
		
		//System.out.println("TESTP :"+key+":"+count);
		//Form Adjacency matrix with Local(within a partition) edges only.
		for (Long src : AdjMatrix.keySet()){
			LocalVertex.add(src);	//List of vertices within given partition
		}
				
		//TreeSet<Long> localSink=new TreeSet<Long>();
		for (Long vertex : LocalVertex){
			for(Long sink: (AdjMatrix.get(vertex)).keySet() ){
				if(! (LocalVertex.contains(sink))){ // TODO: !LocalVertex.contains(sink) then adjMat.get(vertex).get(sink).setRemote(true); 
					(AdjMatrix.get(vertex)).get(sink).setIsRemote();
				}
			}
			//LocalAdjMatrix.put(vertex, localSink); // TODO: remove
		}
		
	LocalVertex.clear();
		
	//	ConnectedComponents cc= new ConnectedComponents();
		ConnectedComponentsNew cc= new ConnectedComponentsNew();
		//System.out.println("TEST: START : call to connected components");
		//Map<Long,Integer> SubgraphMapping = cc.findCC(AdjMatrix); // TODO: pass adjMat //TODO : the subgraphId will be an integer but will be written as Long
		Map<Long,Integer> SubgraphMapping = cc.findWeak(AdjMatrix); // TODO: pass adjMat //TODO : the subgraphId will be an integer but will be written as Long
                //System.out.println("In Partition " + key.get() + " " + SubgraphMapping.keySet().size() + " vertices were assigned a subgraphid");
		//System.out.println("TEST: END : call to connected components");
	//	LocalAdjMatrix.clear(); // TODO: remove
		//Form subgraph ID using partition ID
		//Maintain a vertex to SGid mapping  in the same hashmap 
		int sgid;
		for (Long v :SubgraphMapping.keySet() ){
			sgid = SubgraphMapping.get(v) |((int) (long)key.get()) << 24;
			//long subgraphId = subgraphCount++ | (((long)partitionId) << 32);
			SubgraphMapping.put(v, sgid);  //TODO what should be sgid type??
		}
		//Write the records
		
		/* Reducer Output File :
			SPLAL		P_id, SG_id, V_id, <E_loc, V_loc>+
			SPRAL		P_id, SG_id, V_id, <E_rem, V_rem>+
			SPEL		P_id, SG_id, V_id, <isRemote, E_id, V_sink>+
			SPVL		P_id, SG_id, V_id
			  
			 */
		long partitionId= key.get();long subgraphId; 
		StringBuilder reduceKey=new StringBuilder();
		StringBuilder reduceValue=new StringBuilder();
		StringBuilder localEdges=new StringBuilder();
		StringBuilder remoteEdges=new StringBuilder();
//		long countv = 0L;
//                long countl = 0L;
//                long countr = 0L;
//                Set<Integer> subgraphs = new HashSet<Integer>(SubgraphMapping.values());
		for(Long vertex : SubgraphMapping.keySet()){
//		countv++;
		reduceKey.setLength(0);
		reduceValue.setLength(0);
		localEdges.setLength(0);
		remoteEdges.setLength(0);
			//StringBuilder edges=new StringBuilder();
			
			
			subgraphId= SubgraphMapping.get(vertex);
			
			reduceKey.append(partitionId).append("#").append(subgraphId);
			reduceValue.append(vertex).append("#");
			
			//context.write(new Text(reduceKey.toString()), new Text(reduceValue.toString()));
			//P_id, SG_id, V_id done Write SPVL
			//String SPVLdir=graphID+"/SPVL/";
			//multipleOutputs.write("SPVL", new Text(reduceKey.toString()), new Text(reduceValue.toString()),SPVLdir);
			
			
			String postfix="";
			
			
			 if(!(AdjMatrix.get(vertex).isEmpty())){
				 
				 HashMap<Long,IEdge> childlist =  AdjMatrix.get(vertex);
			
			for(Map.Entry<Long,IEdge> entry : childlist.entrySet() ){
												
				if(!(entry.getValue().checkRemote())){
					// Add this to local edges
//					countl++;
					localEdges.append(postfix).append(entry.getKey()).append(":").append(entry.getValue().getEdgeId());
					//edges.append(postfix).append(0).append(":").append(entry.getKey()).append(":").append(entry.getValue().getEdgeId());
					postfix =",";
					
				}else{
					//Add this to remote edges
//					countr++;
					remoteEdges.append(postfix).append(entry.getKey()).append(":").append(entry.getValue().getEdgeId());
					//edges.append(postfix).append(1).append(":").append(entry.getKey()).append(":").append(entry.getValue().getEdgeId());
					postfix =",";
					
				}

			}
			
			//AdjacencyMatrix.clear();
			//LocalVertex.clear();
			// Write to files
			// SPLAL		P_id, SG_id, V_id, <V_loc, E_loc>+
			
			//multipleOutputs.write(reduceKey.toString(), reduceValue.toString(), "SPLAL");
			//context.write(new Text(reduceKey.toString()), new Text((reduceValue.append(localEdges)).toString()));
			String SPLALdir=graphID+"/SPLAL/";
			String SPRALdir=graphID+"/SPRAL/";
			//String SPELdir=graphID+"/SPVL/";
			multipleOutputs.write("SPLAL",new Text(reduceKey.toString()), new Text((reduceValue.toString() +(localEdges)).toString()),SPLALdir);
			multipleOutputs.write("SPRAL",new Text(reduceKey.toString()), new Text((reduceValue.toString() +(remoteEdges)).toString()),SPRALdir);
			//multipleOutputs.write("SPEL",new Text(reduceKey.toString()), new Text((reduceValue.toString() +(edges)).toString()),SPELdir);
		}
		
	}

//                System.out.println("Partition Id" + key.get() + " Vertex Count: " + countv);
//                System.out.println("Partition Id" + key.get() + " Local Edge Count: " + countl);
//                System.out.println("Partition Id" + key.get() + " Remote Edge Count: " + countr);
//                System.out.println("Partition Id" + key.get() + " Subgraph Count: " + subgraphs.size());
		SubgraphMapping.clear();
		AdjMatrix.clear();
		reduceKey.setLength(0);
		reduceValue.setLength(0);
		localEdges.setLength(0);
		remoteEdges.setLength(0);
		
}
	@Override
	protected void cleanup(Context context)throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		multipleOutputs.close();
	}
	
}
