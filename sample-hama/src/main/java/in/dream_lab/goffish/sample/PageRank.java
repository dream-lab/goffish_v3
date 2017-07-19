package in.dream_lab.goffish.sample;

import com.google.common.collect.Iterables;
import in.dream_lab.goffish.api.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Hullas on 07-07-2017.
 */


public class PageRank extends AbstractSubgraphComputation<LongWritable, LongWritable, LongWritable, LongDoubleWritable, LongWritable, LongWritable, LongWritable> implements ISubgraphWrapup{

    private static final double ALPHA = 0.85;
    private static final double EPSILON = 1e-3;

    private double defaultPR;
    private boolean converged = false;

    private Map<Long, Double> ranks = new HashMap<>();
    private Map<Long, Double> localSums = new HashMap<>();
    private Map<Long, Double> remoteSums = new HashMap<>();
    private Map<Long, Integer> outDegrees = new HashMap<>();

    @Override
    public void compute(Iterable<IMessage<LongWritable, LongDoubleWritable>> iMessages) throws IOException {

        // send my vertex count to all neighbors in 0th superstep
        if(getSuperstep()==0)

            // We are using the vertex ID field to piggy-back the vertex counts, O(SG)
            sendToAll(new LongDoubleWritable(getSubgraph().getLocalVertexCount(), 0.0));

        // Need to initialize PR/degrees and sums, and if not converged perform pagerank.
        else if(getSuperstep()==1){

            // ====================================
            // If first superstep, received count messages and calculate default pagerank.
            // We are using the vertex ID field to piggy-back the vertex counts, O(SG)

            int vertexCount = 0;
            for(IMessage<LongWritable, LongDoubleWritable> message : iMessages)
                vertexCount+=message.getMessage().getVertexId();

            defaultPR = 1.0D/vertexCount;

            // ====================================
            // initialize map for local ranks and outDegrees, and sums from all neighbors, O(E+LV+RV)

            for(IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex : getSubgraph().getVertices()){
                if(!vertex.isRemote()){
                    ranks.put(vertex.getVertexId().get(), defaultPR);

                    // TODO: we can replace this once out degree count is available in API
                    // This will remove the O(E) time complexity, and O(LV) space complexity!
                    int outDegree = Iterables.size(vertex.getOutEdges());
                    outDegrees.put(vertex.getVertexId().get(), outDegree);
                    localSums.put(vertex.getVertexId().get(), 0.0);
                }
                else
                    remoteSums.put(vertex.getVertexId().get(), 0.0);

            }

            pageRankCompute();

        }
        else{
            // ====================================
            // If not first superstep,
            // iterate through messages to test global NOT convergence, and add remote sums
            // Track if us, or even one other has not converged, O(M)

            for (IMessage<LongWritable, LongDoubleWritable> message : iMessages) {
                if (message.getMessage().getVertexId() == -1)
                    // global NOT convergence message seen
                    converged = false;
                else
                    // PR message seen. Add up remote message sums with prior local sums.
                    localSums.put(message.getMessage().getVertexId(),
                        localSums.get(message.getMessage().getVertexId()) + message.getMessage().getSums());

            }

            // all have converged. nothing more to do. End application.
            if(!converged)
                pageRankCompute();
            else
                voteToHalt();
        }
    }

    public void pageRankCompute(){
        // ====================================
        // update local PR values, local/remote sums, local convergence, O(LV)
        if(getSuperstep()!=1) {
            converged = true;
            for (Long vertexId : localSums.keySet()) {
                double pr_old = ranks.get(vertexId);
                ranks.put(vertexId, ALPHA * localSums.get(vertexId) + (1.0 - ALPHA) * defaultPR);
                if (Math.abs(pr_old - ranks.get(vertexId)) > EPSILON)
                    converged = false;
                localSums.put(vertexId, 0.0);
            }
        }

        // if not converged, broadcast special message with NOT converged flag, O(SG)
        if(!converged)
            sendToAll(new LongDoubleWritable(-1,0));

        // ====================================
        // ASSERT: Local sums and remote sums in Map are 0.0. Local PR value is updated.

        // ====================================
        // update sums for local and remote vertices, O(E+LV+RV)
        for(IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex : getSubgraph().getLocalVertices()){
            double weight = ranks.get(vertex.getVertexId().get()) / outDegrees.get(vertex.getVertexId().get());
            for(IEdge<LongWritable, LongWritable, LongWritable> edge : vertex.getOutEdges()) {
                if (getSubgraph().getVertexById(edge.getSinkVertexId()).isRemote())
                    remoteSums.put(edge.getSinkVertexId().get(),
                        remoteSums.get(edge.getSinkVertexId().get()) + weight);
                else
                    localSums.put(edge.getSinkVertexId().get(),
                            localSums.get(edge.getSinkVertexId().get()) + weight);
            }
        }

        // Send PR message to all remote neighbors, O(RV)
        for (IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable> remote : getSubgraph()
                .getRemoteVertices()) {
            long remoteId = remote.getVertexId().get();
            sendMessage(remote.getSubgraphId(), new LongDoubleWritable(remoteId, remoteSums.get(remoteId)));
            remoteSums.put(remoteId, 0.0); // Init remote sum once message sent
        }
    }

    @Override
    public void wrapup() throws IOException {
        for(Long vertexId : ranks.keySet())
            System.out.println(vertexId + " PR: " + ranks.get(vertexId));
    }
}

class LongDoubleWritable implements Writable {

    private long vertexId;
    private double sums;

    public LongDoubleWritable(){}

    LongDoubleWritable(long vertexId, double sums) {
        this.vertexId = vertexId;
        this.sums = sums;
    }

    public long getVertexId() {
        return vertexId;
    }

    public double getSums() {
        return sums;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(vertexId);
        dataOutput.writeDouble(sums);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        vertexId = dataInput.readLong();
        sums = dataInput.readDouble();
    }
}