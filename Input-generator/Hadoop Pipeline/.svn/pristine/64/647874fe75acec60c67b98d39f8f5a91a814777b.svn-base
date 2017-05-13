package in.dream_lab.hadoopPipeline.cc;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * Mapper will read SNAP input file and map with vertex as key, the adjacency list of that vertex
 * 
 * Input: 
 * 	SNAP data (Edge list)
 *  
 * 
 * Output:
 *  
 *  <V, Adjacent vertices of V>
 * 
 */
public class VertexMapper extends Mapper <Object , Text, LongWritable, LongWritable> {

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		//From each line extract source & sink 
		
		String line=value.toString();
		if(!line.startsWith("#")){
            
            String[] strs = line.trim().split("\\s+");
            LongWritable src=new LongWritable(Long.parseLong(strs[0]));
            LongWritable sink=new LongWritable(Long.parseLong(strs[1]));
            context.write(src,sink);
            
		}
				
	}

	
	
}
