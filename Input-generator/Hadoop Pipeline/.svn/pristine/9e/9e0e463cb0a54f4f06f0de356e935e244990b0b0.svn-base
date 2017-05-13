package in.dream_lab.hadoopPipeline.cc;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.JobID;
/*
 * @uthor:RavikantDindokar
 * Job ID : 1
 * Job Name : EL_to_UEL
 * Job Description: Convert directed to undirected graph
 * Map Input File: EL (SNAP file)
 * Map Input Format :V_src, V_sink
 * Map Emit :V_src, [V_sink]  && V_sink, [V_src]
 * Reducer Emit: V_src, E_id:V_sink && V_sink, E_id:V_src
 * Reducer Output File :UEL
 * Note :Remove duplicates
 * EdgeId : first  8 bits : partitionId, next  40bits : local count , last 16 bits : left for future use(instance data)
 */

public class BlogelReducer extends Reducer< LongWritable, LongWritable,LongWritable,Text> {

    //public static long localcount= 0L;
    @Override
    protected void reduce(LongWritable key, Iterable<LongWritable> values , Context context)
            throws IOException, InterruptedException {
		

        LongWritable sourceId=key;
        Set<Long> adjlist = new HashSet<Long>();

        Long number_of_neighbours=0L;

        for(LongWritable v : values){
            adjlist.add(v.get());
            number_of_neighbours++;
        }


        StringBuilder sb =new StringBuilder();

        sb.append(number_of_neighbours);

        String prefix = " ";

        for(Long sinkId : adjlist){

            sb.append(prefix);

            //prefix = " ";

            sb.append(sinkId);

        }

        context.write(sourceId, new Text(sb.toString()));
    }

}
