package in.dream_lab.goffish;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import java.io.IOException;

/**
 * Created by Hullas on 03-07-2017.
 */
public class MyOutputWriter
        extends IdWithValueTextOutputFormat<IntWritable, MyIntMapWritable, NullWritable> {
    public MyOutputWriter() {
    }

    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
        return new MyOutputWriter.MyVertexValueWriter();
    }

    protected class MyVertexValueWriter extends
            IdWithValueTextOutputFormat<IntWritable, MyIntMapWritable, NullWritable>.IdWithValueVertexWriter{

        protected MyVertexValueWriter() {
            super();
        }

        @Override
        protected Text convertVertexToLine(Vertex<IntWritable, MyIntMapWritable, NullWritable> vertex) throws IOException {
            StringBuilder str = new StringBuilder();
            str.append(vertex.getId().get());
            str.append(" ");
            str.append(vertex.getValue().getINT());

            return new Text(str.toString());
        }
    }
}
