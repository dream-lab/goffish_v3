package in.dream_lab.hadoopPipeline.cc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
/**
 * Method to find subgraphs (WCC) within a partition
 * 
 * Input: 
 * 	SNAP data (Edge list)
 *  
 * Read
 * 	Partition file (Vi,Pk)
 * 
 * Output:
 *  
 *  Map each vertex to reducer with partition id.
 * 
 */

public class FindSubgraphMapper extends Mapper <Object , Text, LongWritable, LongWritable> {

    @Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
    		
    	String line=value.toString();
    	String[] strs = line.trim().split("\\s+");
    	long partitionId=Long.parseLong(strs[1]);
    	long vid=Long.parseLong(strs[0]);
    	 context.write(new LongWritable(partitionId), new LongWritable(vid));
    }
}
