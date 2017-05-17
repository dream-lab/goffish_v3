package in.dream_lab.hadoopPipeline.cc;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class EdgeListUndirected extends Reducer< LongWritable, LongWritable,LongWritable,LongWritable>  {

	@Override
	protected void reduce(LongWritable key, Iterable<LongWritable> values , Context context)
			throws IOException, InterruptedException {
		
		LongWritable sourceId=key;
		Set<Long> adjlist = new HashSet<Long>();
		for(LongWritable v : values){
			adjlist.add(v.get());
		}
		
		for(Long sinkId : adjlist){
			
			context.write(sourceId, new LongWritable(sinkId));
		}
		
	}

}
