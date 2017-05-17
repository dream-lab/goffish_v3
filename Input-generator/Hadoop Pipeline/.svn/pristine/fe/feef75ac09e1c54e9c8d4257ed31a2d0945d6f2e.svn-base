package in.dream_lab.hadoopPipeline.cc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
/**
 * Method to find subgraphs (WCC) within a partition
 * 
 * Input: 
 * 	Edge list : SNAP format (only internal edges for the partition)
 * 
 * Read:
 * 	Partition file :(vertex_id, partition_id) //A single partition
 * 
 * Output:
 *  CC in the partition.
 *  
 * Data :
 * MultiMap mhm : Adjacency list of vertices <Vertex,List<Vertex>>
 * HashMap SubgraphMapping : <SubgraphId,List <Vertex>> 
 * 
 */

public class FindSubgraphReducer extends Reducer < LongWritable, LongWritable,Text,Text>{

	@Override
	protected void reduce(LongWritable key, Iterable<LongWritable> values , Context context)
			throws IOException, InterruptedException {
	
		LongWritable partitionId=key;
		System.out.println("TESTKEY "+partitionId);
		Multimap<Long, Long> mhm = HashMultimap.create();
		
		for(LongWritable v : values){
			System.out.println("TESTVALUE "+v+" "+partitionId);
			mhm.put(v.get(), null);
		}
		//long min=Long.MAX_VALUE;
		//Read SNAP data for the partition
		try{
            Path pt=new Path("hdfs://orion-00:9000/user/bduser/tiny-snap.txt");
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            line=br.readLine();
            while (line != null){
                    System.out.println("TESTLINE "+line);
                    String[] strs = line.trim().split("\\s+");
                    long v1=Long.parseLong(strs[0]);
                    long v2=Long.parseLong(strs[1]);
                    System.out.println("TESTV "+v1 + " "+v2);
                    if(mhm.containsKey(v1) && mhm.containsKey(v2) ){
            /*        if(min> v1)min=v1;
                    if(min>v2)min=v2;*/
                    mhm.put(v1, v2);	
                    mhm.put(v2, v1);	
                    }  
                    
                    line=br.readLine();
            }
		}catch(Exception e){
    }
		//Run WCC to identify subgraphs 
		HashMap<Long,Long> SubgraphMapping=new HashMap<Long, Long>();
		ConnectedComponents cc=new ConnectedComponents();
		
		Set<Long> keySet = mhm.keySet();
	    Iterator<Long> keyIterator = keySet.iterator();
	    while (keyIterator.hasNext() ) {
	        Long key1 = (Long)keyIterator.next();
	        Set<Long> childlist = (Set<Long>) mhm.get(key1);
	       for(Long e : childlist){
	    	   System.out.println("MHM "+ key+" "+e);
	       }
	    }
		
		
		
		//SubgraphMapping=cc.findCC(mhm);
		Multimap<Long, Long> multiMap = HashMultimap.create();
		for (Entry<Long, Long> entry : SubgraphMapping.entrySet()) {
			long subgraphId =  entry.getValue()| ((partitionId.get()) << 32);
			context.write(new Text(Long.toString(subgraphId)),new Text(Long.toString(entry.getKey())));
			multiMap.put(subgraphId, entry.getKey());
		}
	}
}
