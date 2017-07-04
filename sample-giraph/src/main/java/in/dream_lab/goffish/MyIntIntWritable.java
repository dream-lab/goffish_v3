package in.dream_lab.goffish;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Hullas on 02-07-2017.
 */
public class MyIntIntWritable implements Writable {
    public int id1;
    public int id2;

    public MyIntIntWritable() {
    }

    public MyIntIntWritable(int id1, int id2) {
        this.id1 = id1;
        this.id2 = id2;
    }

    public int getId1() {
        return this.id1;
    }

    public int getId2() {
        return this.id2;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.id1);
        dataOutput.writeInt(this.id2);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.id1 = dataInput.readInt();
        this.id2 = dataInput.readInt();
    }
}