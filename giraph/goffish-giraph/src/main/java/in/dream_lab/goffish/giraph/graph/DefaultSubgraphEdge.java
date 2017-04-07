package in.dream_lab.goffish.giraph.graph;

import in.dream_lab.goffish.api.IEdge;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by anirudh on 18/10/16.
 */
public class DefaultSubgraphEdge<I extends WritableComparable, E extends Writable, EI extends WritableComparable> implements IEdge<E, I, EI>, Writable {

    private EI edgeId;
    private I sinkVertexId;
    private E edgeValue;

    public void initialize(EI edgeId, E edgeValue, I sinkVertexId) {
        this.edgeId = edgeId;
        this.edgeValue = edgeValue;
        this.sinkVertexId = sinkVertexId;
    }

    @Override
    public I getSinkVertexId() {
        return sinkVertexId;
    }

    @Override
    public E getValue() {
        return edgeValue;
    }

    @Override
    public EI getEdgeId() {
        return edgeId;
    }

    @Override
    public void setValue(E value) {
        this.edgeValue = value;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object obj) {
        return this.getSinkVertexId().equals(((IEdge)obj).getSinkVertexId());
    }
}
