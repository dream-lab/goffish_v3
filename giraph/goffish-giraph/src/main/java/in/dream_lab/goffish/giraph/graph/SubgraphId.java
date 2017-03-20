package in.dream_lab.goffish.giraph.graph;

import in.dream_lab.goffish.giraph.conf.GiraphSubgraphConfiguration;
import org.apache.giraph.conf.GiraphConfigurationSettable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by anirudh on 29/09/16.
 */
public class SubgraphId<S extends WritableComparable> implements WritableComparable, GiraphConfigurationSettable {
    private int partitionId;
    private S subgraphId;
    private GiraphSubgraphConfiguration<S, ?, ?, ?, ?, ?> conf;
    public SubgraphId() {
    }

    public SubgraphId(S subgraphId, int partitionId) {
        this.partitionId = partitionId;
        this.subgraphId = subgraphId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public S getSubgraphId() {
        return subgraphId;
    }

    @Override
    public int compareTo(Object o) {
        SubgraphId other = (SubgraphId) o;
        return subgraphId.compareTo(other.getSubgraphId());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        subgraphId.write(dataOutput);
        dataOutput.writeInt(partitionId);
    }

    @Override
    public int hashCode() {
        return subgraphId.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof SubgraphId))
            return false;
        SubgraphId other = (SubgraphId) obj;
        return subgraphId.equals(other.subgraphId);
    }

    /*   EITHER DO THIS OR


        import java.lang.reflect.ParameterizedType;

            class Foo {

                public bar() {
                    ParameterizedType superClass = (ParameterizedType) getClass().getGenericSuperclass();
                    Class type = (Class) superClass.getActualTypeArguments()[0];
                    try {
                        T t = type.newInstance();
                        //Do whatever with t
                    } catch (Exception e) {
                        // Oops, no default constructor
                        throw new RuntimeException(e);
                    }
                }
            }

            OR
            make subgraph class abstract and extend it for custom types

        */

    public void readFields(DataInput dataInput) throws IOException {
      subgraphId = conf.createSubgraphId();
      System.out.println("Haha SubgraphIdClass" + subgraphId.getClass().getSimpleName());
      subgraphId.readFields(dataInput);
      partitionId = dataInput.readInt();
    }

    @Override
    public void setConf(ImmutableClassesGiraphConfiguration configuration) {
        conf = new GiraphSubgraphConfiguration<>(configuration);
    }
}
