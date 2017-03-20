package in.dream_lab.goffish.giraph.factories;

import org.apache.giraph.factories.ValueFactory;
import org.apache.hadoop.io.Writable;

/**
 * Created by anirudh on 29/11/16.
 */
public interface SubgraphMessageValueFactory<M extends Writable>
    extends ValueFactory<M> {
}
