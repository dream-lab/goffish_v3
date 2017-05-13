package in.dream_lab.hadoopPipeline.cc;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class VornoiInputReaderMapper extends Mapper<Object , Text, Text, Text> {
	
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		
		String line=value.toString();
		
		String[] strs = line.trim().split("\\s+");
		
//		take the source vertex  information

		String sourceTuple=strs[0]+","+strs[1]+","+strs[2];
		//for each adjacent vertex write Vsrc,Vsink
		
		
		for (int i1=3;i1<strs.length;i1=i1+3){

			String sinkTuple=strs[i1]+","+strs[i1+1]+","+strs[i1+2];
			
			
			context.write(new Text(sourceTuple), new Text(sinkTuple));
//			
		}
		
			
		
	}

	
}
