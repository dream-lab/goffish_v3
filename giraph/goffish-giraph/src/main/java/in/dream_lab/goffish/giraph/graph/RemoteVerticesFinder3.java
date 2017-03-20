package in.dream_lab.goffish.giraph.graph;

import in.dream_lab.goffish.api.IMessage;
import in.dream_lab.goffish.api.IRemoteVertex;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.ExtendedByteArrayDataInput;
import org.apache.hadoop.io.*;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by anirudh on 06/11/16.
 */
public class RemoteVerticesFinder3 extends GiraphSubgraphComputation<LongWritable,
    LongWritable, DoubleWritable, DoubleWritable, BytesWritable, NullWritable, LongWritable> {
  @Override
  public void compute(Vertex<SubgraphId<LongWritable>, SubgraphVertices<NullWritable, DoubleWritable, DoubleWritable, LongWritable, LongWritable, LongWritable>, DoubleWritable> vertex, Iterable<SubgraphMessage<LongWritable,BytesWritable>> subgraphMessages) throws IOException {
    DefaultSubgraph<NullWritable, DoubleWritable, DoubleWritable, LongWritable, LongWritable, LongWritable> subgraph = (DefaultSubgraph) vertex;
    HashMap<LongWritable, IRemoteVertex<DoubleWritable, DoubleWritable, LongWritable, LongWritable, LongWritable>> remoteVertices = subgraph.getSubgraphVertices().getRemoteVertices();
    //System.out.println("IN RVF 3\n");
    for (IMessage<LongWritable,BytesWritable> message : subgraphMessages) {
      ExtendedByteArrayDataInput dataInput = new ExtendedByteArrayDataInput(message.getMessage().getBytes());
      SubgraphId<LongWritable> senderSubgraphId = org.apache.giraph.utils.WritableUtils.createWritable(SubgraphId.class, getConf());
      senderSubgraphId.readFields(dataInput);
      //System.out.println("Message received from subgraph  ID :" + senderSubgraphId.getSubgraphId() + "to subgraph :"+subgraph.getVertexId().getSubgraphId());
      int numVertices = dataInput.readInt();
      //System.out.println("numvertices received in this message are : "+ numVertices);
      for (int i = 0; i < numVertices; i++) {
        DefaultRemoteSubgraphVertex rsv = new DefaultRemoteSubgraphVertex();
        LongWritable rsvId = new LongWritable();
        rsvId.readFields(dataInput);
        //System.out.println("Remote Edge: From subgraph " + subgraph.getVertexId().getSubgraphId() + " is To vertex : " + rsvId +" in neighbor subgraph with ID: " + senderSubgraphId);
        rsv.setSubgraphId(senderSubgraphId.getSubgraphId());
        rsv.setId(rsvId);
        remoteVertices.put(rsvId, rsv);
      }
    }
  }
}


