package in.dream_lab.hadoopPipeline.cc;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class VornoiInputReaderReducer extends Reducer< Text, Text,Text,Text> {

	public static long localcount= 0L;
	@Override
	protected void reduce(Text key, Iterable<Text> values , Context context)
			throws IOException, InterruptedException {
		
		
		/*final String jobId = System.getenv("mapred_job_id");
		String[] strs=jobId.split("_");
		
		String reducerStr =strs[strs.length - 1];
		long reducerId=Long.parseLong(reducerStr);
		
		//System.out.println("TEST : TASKId :"+context.getTaskAttemptID().getTaskID().getId()+ " reducerID :"+reducerStr);
		*/
		String  sourcetuple = key.toString();
		
		
		Set<AdjVertex> adjlist = new HashSet<AdjVertex>();
		
		for(Text  v : values){
			String[] strs = v.toString().split(",");
			AdjVertex neighbour=new AdjVertex(Long.parseLong(strs[0]), Long.parseLong(strs[1]), Integer.parseInt(strs[2]));
			adjlist.add(neighbour);
		}
		
				
		long taskId=context.getTaskAttemptID().getTaskID().getId();
		
		long edgeId;
		
		String edgeSinkPair="";
		
		StringBuilder sb =new StringBuilder();
		
		String prefix = "";
		
		for(AdjVertex  sink : adjlist){
			
			localcount++;
			
			edgeId=(localcount <<16)|(taskId << 55);
			
			sb.append(prefix);
			 
			prefix = ":";
			
			edgeSinkPair= edgeId +","+sink.getid()+","+sink.getsgid()+","+sink.getpid();
			
			sb.append(edgeSinkPair);
			
		}
	
		context.write(new Text(sourcetuple), new Text(sb.toString()));
	}

}

class AdjVertex{

	   Long id;
	   Long sgid;
	   int pid;

	   public AdjVertex(Long a, Long b, int c)
	   {
	    this.id = a;
	    this.sgid = b;
	    this.pid = c;
	   }

	   Long getid(){ return id;}
	   Long getsgid(){ return sgid;}
	   int getpid(){ return pid;}
	
}