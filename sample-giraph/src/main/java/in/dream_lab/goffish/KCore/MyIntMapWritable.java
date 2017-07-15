package in.dream_lab.goffish;

import org.apache.hadoop.io.AbstractMapWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * Created by Hullas on 02-07-2017.
 */

public class MyIntMapWritable extends MapWritable implements Writable {
    int INT;
    Map<IntWritable, IntWritable> MAP;

    public MyIntMapWritable() {
    }

    public MyIntMapWritable(int INT, Map<IntWritable, IntWritable> MAP) {
        this.INT = INT;
        this.MAP = MAP;
    }

    public int getINT() {
        return INT;
    }

    public Map<IntWritable, IntWritable> getMAP() {
        return MAP;
    }

    public void write(DataOutput dataOutput) throws IOException {
        super.write(dataOutput);
        dataOutput.writeInt(this.INT);
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        this.INT = in.readInt();
    }
}