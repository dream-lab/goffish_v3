package in.dream_lab.hadoopPipeline.cc;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class VertexReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

	@Override
	protected void reduce(LongWritable key, Iterable<LongWritable> values , Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		
		
		
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		
	}

}
