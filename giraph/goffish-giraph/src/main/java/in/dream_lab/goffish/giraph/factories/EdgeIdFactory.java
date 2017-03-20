package in.dream_lab.goffish.giraph.factories;

import org.apache.giraph.factories.ValueFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Created by anirudh on 23/10/16.
 */
public interface EdgeIdFactory<EI extends WritableComparable> extends ValueFactory<EI> {
}
