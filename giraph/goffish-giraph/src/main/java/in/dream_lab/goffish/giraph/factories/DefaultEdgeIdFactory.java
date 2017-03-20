package in.dream_lab.goffish.giraph.factories;

import org.apache.giraph.conf.GiraphConfigurationSettable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.WritableComparable;

/**
 * Created by anirudh on 23/10/16.
 */
public class DefaultEdgeIdFactory<EI extends WritableComparable> implements EdgeIdFactory<EI>, GiraphConfigurationSettable {

    private ImmutableClassesGiraphConfiguration conf;
    private Class<EI> edgeIdClass;

    @Override
    public EI newInstance() {
        return WritableUtils.createWritable(edgeIdClass, conf);
    }

    @Override
    public void setConf(ImmutableClassesGiraphConfiguration configuration) {
        this.conf = configuration;
//        edgeIdClass = conf.getEdgeIdClass();
    }
}
