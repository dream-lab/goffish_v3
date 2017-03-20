package in.dream_lab.goffish.giraph.factories;

import org.apache.giraph.conf.GiraphConfigurationSettable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;

/**
 * Created by anirudh on 23/10/16.
 */
public class DefaultSubgraphValueFactory<SV extends Writable> implements SubgraphValueFactory<SV>, GiraphConfigurationSettable {

    private ImmutableClassesGiraphConfiguration conf;
    private Class<SV> subgraphValueClass;

    @Override
    public void setConf(ImmutableClassesGiraphConfiguration configuration) {
        this.conf = configuration;
//        subgraphValueClass = conf.getSubgraphValueClass();
    }

    @Override
    public SV newInstance() {
        return WritableUtils.createWritable(subgraphValueClass, conf);
    }

}
