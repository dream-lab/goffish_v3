package in.dream_lab.goffish.giraph.factories;

import in.dream_lab.goffish.giraph.graph.SubgraphId;
import org.apache.giraph.partition.GraphPartitionerFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Created by anirudh on 29/09/16.
 */
public class SubgraphPartitionerFactory<I extends WritableComparable,
        V extends Writable, E extends Writable>
        extends GraphPartitionerFactory<I, V, E> {

    // TODO: Assuming that the output would be such that each subgraph ends up in a single giraph partition
    @Override
    public int getPartition(I id, int partitionCount, int workerCount) {
        SubgraphId subgraphId = (SubgraphId) id;
        return subgraphId.getPartitionId() % partitionCount;
    }

    // TODO: Ideally metis partition should end up in same worker. How to do that?
    // Given partition(subgraph id in our case) need a way to make all subgraph belonging to
    // the same partition end up in the worker
    @Override
    public int getWorker(int partition, int partitionCount, int workerCount) {
        return partition % workerCount;
    }
}