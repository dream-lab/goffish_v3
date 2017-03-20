package in.dream_lab.goffish.giraph.factories;

import org.apache.giraph.factories.ValueFactory;
import org.apache.hadoop.io.Writable;

/**
 * Created by anirudh on 23/10/16.
 */
public interface SubgraphVertexValueFactory<SVV extends Writable> extends ValueFactory<SVV> {

}
