package in.dream_lab.goffish.giraph.formats;

import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.giraph.graph.SubgraphId;
import in.dream_lab.goffish.giraph.graph.SubgraphVertices;
import in.dream_lab.goffish.giraph.graph.DefaultSubgraphEdge;
import in.dream_lab.goffish.giraph.graph.DefaultSubgraphVertex;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.AdjacencyListTextVertexInputFormat;
import org.apache.giraph.io.formats.TextDoubleDoubleAdjacencyListVertexInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by anirudh on 30/09/16.
 */
public class LongDoubleDoubleAdjacencyListSubgraphInputFormat extends AdjacencyListTextSubgraphInputFormat<SubgraphId<LongWritable>, SubgraphVertices,
        DoubleWritable> {
    @Override
    public AdjacencyListTextSubgraphReader createVertexReader(InputSplit split,
                                                            TaskAttemptContext context) {
        return new LongDoubleDoubleAdjacencyListSubgraphReader(null);
    }

    /**
     * Vertex reader used with
     * {@link TextDoubleDoubleAdjacencyListVertexInputFormat}
     */
    protected class LongDoubleDoubleAdjacencyListSubgraphReader extends
            AdjacencyListTextSubgraphReader {

        @Override
        public IEdge<NullWritable, LongWritable, NullWritable> decodeVertexEdge(String id) {
            LongWritable vertexId = new LongWritable(Long.parseLong(id));
            DefaultSubgraphEdge<LongWritable, NullWritable, NullWritable> subgraphEdge = new DefaultSubgraphEdge<>();
            subgraphEdge.initialize(NullWritable.get(), NullWritable.get(), vertexId);
            return subgraphEdge;
        }

        @Override
        public SubgraphVertices getSubgraphVertices() throws IOException, InterruptedException {
            SubgraphVertices<LongWritable, DoubleWritable, DoubleWritable, LongWritable, LongWritable, LongWritable> subgraphVertices = new SubgraphVertices();
            HashMap<LongWritable, IVertex<DoubleWritable, DoubleWritable, LongWritable, LongWritable>> subgraphVerticesMap = new HashMap<>();
            while (getRecordReader().nextKeyValue()) {
                // take all info from each line

                // Read each vertex
                Text vertexLine = getRecordReader().getCurrentValue();
                String[] processedLine = preprocessLine(vertexLine);

                IVertex<DoubleWritable, DoubleWritable, LongWritable, LongWritable> subgraphVertex = readVertex(processedLine);
                subgraphVerticesMap.put(subgraphVertex.getVertexId(), subgraphVertex);
            }
            subgraphVertices.initialize(subgraphVerticesMap);
            subgraphVertices.setSubgraphValue(new LongWritable());
            return subgraphVertices;
        }

        @Override
        public Edge<SubgraphId<LongWritable>, DoubleWritable> decodeSubgraphEdge(String value1, String value2) {
            LongWritable sid = new LongWritable(Long.parseLong(value1));
            int pid = Integer.parseInt(value2);
            SubgraphId<LongWritable> subgraphId = new SubgraphId<>(sid, pid);
            Edge <SubgraphId<LongWritable>, DoubleWritable> edge = EdgeFactory.create(subgraphId, new DoubleWritable(0));
            return edge;
        }


        @Override
        public int decodePId(String s) {
            return Integer.parseInt(s);
        }

        /**
         * Constructor with
         * {@link AdjacencyListTextVertexInputFormat.LineSanitizer}.
         *
         * @param lineSanitizer the sanitizer to use for reading
         */
        public LongDoubleDoubleAdjacencyListSubgraphReader(AdjacencyListTextSubgraphInputFormat.LineSanitizer
                                                                 lineSanitizer) {
            super(lineSanitizer);
        }

        @Override
        public LongWritable decodeId(String s) {
            return new LongWritable(Long.parseLong(s));
        }

        @Override
        public LongWritable decodeSId(String s) {
            return new LongWritable(Long.parseLong(s));
        }


        @Override
        public IVertex readVertex(String[] line) throws IOException{
            DefaultSubgraphVertex subgraphVertex = new DefaultSubgraphVertex();
            subgraphVertex.initialize(getVId(line), getValue(line), getVertexEdges(line));
            return subgraphVertex;
        }




        @Override
        public DoubleWritable decodeValue(String s) {
            return new DoubleWritable(Double.parseDouble(s));
        }

        @Override
        public SubgraphId<LongWritable> getSId(String[] line) {
            SubgraphId<LongWritable> subgraphId = new SubgraphId<LongWritable>(decodeSId(line[0]), decodePId(line[1]));
            return subgraphId;
        }
    }
}
