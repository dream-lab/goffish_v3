package in.dream_lab.goffish.sample;

import com.google.common.collect.Iterables;
import in.dream_lab.goffish.api.*;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Hullas on 08-07-2017.
 */

public class SubgraphRank extends AbstractSubgraphComputation<LongWritable, LongWritable, LongWritable, LongDoubleWritable, LongWritable, LongWritable, LongWritable> implements ISubgraphWrapup{

    private static final double ALPHA = 0.85;
    private static final double EPSILON = 1e-3;

    private double defaultPR;
    private boolean converged =false;

    private Map<Long, Double> ranks = new HashMap<>();
    private Map<Long, Double> localSums = new HashMap<>();
    private Map<Long, Double> remoteSums = new HashMap<>();
    private Map<Long, Integer> outDegrees = new HashMap<>();

    @Override
    public void compute(Iterable<IMessage<LongWritable, LongDoubleWritable>> iMessages) throws IOException {

        // send my vertex count to all neighbors in 0th superstep
        if(getSuperstep()==0) {

            // ====================================
            // Local Page Rank
            LPRCompute();

            // We are using the vertex ID field to piggy-back the vertex counts, O(SG)
            sendToAll(new LongDoubleWritable(getSubgraph().getLocalVertexCount(), 0.0));
        }

        else if(getSuperstep()==1){

            // ====================================
            // If first superstep, receive count messages and calculate block-adjusted pagerank.
            // We are using the vertex ID field to piggy-back the vertex counts, O(SG)
            // Need to initialize PR/degrees and sums, and if not converged perform pagerank.

            int globalVertexCount=0;
            for(IMessage<LongWritable, LongDoubleWritable> message : iMessages)
                globalVertexCount+=message.getMessage().getVertexId();

            double SG_by_G = ((double) getSubgraph().getLocalVertexCount())/globalVertexCount;
            // Global default PR
            defaultPR = 1/globalVertexCount;

            // ====================================
            // Update block rank and init local+remote sums/degree map before switch to global PR
            for(Long vertex : ranks.keySet())
                ranks.put(vertex, ranks.get(vertex)*SG_by_G);

            GPRCompute();
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

            if(!converged)
                GPRCompute();
            else
                // all have converged. nothing more to do. End application.
                voteToHalt();
        }
    }

    public void LPRCompute() {
        final double EPSILON_LPR = 0.05;
        Map<Long, Integer> localOutDegrees = new HashMap<>();

        double localDefaultPR = 1.0D / getSubgraph().getLocalVertexCount();

        // initialize map for local ranks and local outDegrees, and sums from local neighbors
        for(IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex : getSubgraph().getVertices()) {
            if(!vertex.isRemote()) {
                ranks.put(vertex.getVertexId().get(), localDefaultPR);
                localSums.put(vertex.getVertexId().get(), 0.0);
                outDegrees.put(vertex.getVertexId().get(), Iterables.size(vertex.getOutEdges()));

                // TODO: we can replace this once local out degree count is available in API
                int localOutDegree = 0;
                for (IEdge<LongWritable, LongWritable, LongWritable> edge : vertex.getOutEdges()) {
                    if (!getSubgraph().getVertexById(edge.getSinkVertexId()).isRemote())
                        localOutDegree++;
                }
                localOutDegrees.put(vertex.getVertexId().get(), localOutDegree);
            }
            else
                remoteSums.put(vertex.getVertexId().get(), 0.0);
        }

        // ====================================
        // Do local PR till convergence
        do {

            // update sums for local and remote vertices, O(E+LV+RV)
            for (IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex : getSubgraph().getLocalVertices()) {
                double weight = ranks.get(vertex.getVertexId().get()) / localOutDegrees.get(vertex.getVertexId().get());
                for (IEdge<LongWritable, LongWritable, LongWritable> edge : vertex.getOutEdges()) {
                    if (!getSubgraph().getVertexById(edge.getSinkVertexId()).isRemote())
                        // for local edges only
                        localSums.put(edge.getSinkVertexId().get(),
                            localSums.get(edge.getSinkVertexId().get()) + weight);
                }
            }
            converged = true;
            // update local PR values
            for (Long vertexId : localSums.keySet()) {
                double pr_old = ranks.get(vertexId);
                ranks.put(vertexId, ALPHA * localSums.get(vertexId) + (1.0 - ALPHA) * localDefaultPR);
                if (Math.abs(pr_old - ranks.get(vertexId)) > EPSILON_LPR)
                    converged = false;
                localSums.put(vertexId, 0.0);
            }
        }while(!converged);

        // for global convergence
        converged =false;
    }

    public void GPRCompute(){
        // ====================================
        // update local PR values, local/remote sums, local convergence, O(LV)
        if(getSuperstep()!=1) {
            converged = true;
            for (Long vertexId : localSums.keySet()) {
                double pr_old = ranks.get(vertexId);
                ranks.put(vertexId, ALPHA * localSums.get(vertexId) + (1.0 - ALPHA) * defaultPR);
                if (Math.abs(pr_old - ranks.get(vertexId)) > EPSILON)
                    converged = false;
                localSums.put(vertexId, 0.0); // reset local sum for this vertex
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