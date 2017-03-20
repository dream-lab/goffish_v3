package in.dream_lab.goffish.giraph.conf;

import in.dream_lab.goffish.giraph.factories.*;
import org.apache.giraph.conf.BooleanConfOption;
import org.apache.giraph.conf.ClassConfOption;
import org.apache.giraph.conf.LongConfOption;
import org.apache.hadoop.io.*;

/**
 * Created by anirudh on 09/03/17.
 */
public interface GiraphSubgraphConstants {
  // subgraph Value factory class -- OURS
   ClassConfOption<SubgraphValueFactory> SUBGRAPH_VALUE_FACTORY_CLASS =
          ClassConfOption.create("giraph.subgraphValueFactoryClass",
                  DefaultSubgraphValueFactory.class, SubgraphValueFactory.class,
                  "Subgraph Value Factory class- optional");
  ClassConfOption<SubgraphVertexValueFactory> SUBGRAPH_VERTEX_VALUE_FACTORY_CLASS =
          ClassConfOption.create("giraph.subgraphVertexValueFactoryClass",
                  DefaultSubgraphVertexValueFactory.class, SubgraphVertexValueFactory.class,
                  "Subgraph Value Factory class- optional");
  ClassConfOption<SubgraphIdFactory> SUBGRAPH_ID_FACTORY_CLASS =
          ClassConfOption.create("giraph.subgraphIdFactoryClass",
                  DefaultSubgraphIdFactory.class, SubgraphIdFactory.class,
                  "Subgraph ID Factory class - optional");
  ClassConfOption<SubgraphVertexIdFactory> SUBGRAPH_VERTEX_ID_FACTORY_CLASS =
      ClassConfOption.create("giraph.subgraphIdFactoryClass",
          DefaultSubgraphVertexIdFactory.class, SubgraphVertexIdFactory.class,
          "Subgraph Vertex ID Factory class - optional");
  ClassConfOption<SubgraphMessageValueFactory> SUBGRAPH_MESSAGE_VALUE_FACTORY_CLASS =
      ClassConfOption.create("giraph.subgraphMessageValueFactoryClass",
          DefaultSubgraphMessageValueFactory.class, SubgraphMessageValueFactory.class,
          "Subgraph Vertex ID Factory class - optional");
  // ours
    BooleanConfOption IS_SUBGRAPH_COMPUTATION =
          new BooleanConfOption("giraph.subgraphComputation", false,
                  "Use Giraph for Subgraph input");
  ClassConfOption<WritableComparable> SUBGRAPH_ID_CLASS =
            ClassConfOption.create("giraph.subgraphIdClass",
                    LongWritable.class, WritableComparable.class,
                    "Subgraph ID class");
  ClassConfOption<WritableComparable> SUBGRAPH_VERTEX_ID_CLASS =
          ClassConfOption.create("giraph.subgraphVertexIdClass",
                  LongWritable.class, WritableComparable.class,
                  "Vertex ID class");
  ClassConfOption<Writable> SUBGRAPH_MESSAGE_VALUE_CLASS =
      ClassConfOption.create("giraph.subgraphMessageValueClass",
          LongWritable.class, Writable.class,
          "Subgraph message value class");
  ClassConfOption<Writable> SUBGRAPH_VALUE_CLASS =
          ClassConfOption.create("giraph.subgraphValueClass",
                  DoubleWritable.class, Writable.class,
                  "Subgraph value class");
  ClassConfOption<Writable> SUBGRAPH_VERTEX_VALUE_CLASS =
          ClassConfOption.create("giraph.subgraphVertexValueClass",
                  DoubleWritable.class, Writable.class,
                  "Subgraph vertex value class");
  ClassConfOption<Writable> SUBGRAPH_VERTEX_EDGE_VALUE_CLASS =
      ClassConfOption.create("giraph.subgraphEdgeValueClass",
          NullWritable.class, Writable.class,
          "Subgraph vertex value class");LongConfOption SUBGRAPH_SOURCE_VERTEX =
                  new LongConfOption("giraph.subgraphSourceVertex", 0, "Source vertex for algorithms like SSP");
  ClassConfOption<WritableComparable> SUBGRAPH_EDGE_ID_CLASS =
          ClassConfOption.create("giraph.edgeIdClass",
                  LongWritable.class, WritableComparable.class,
                  "Edge ID class");
  ClassConfOption<EdgeIdFactory> EDGE_ID_FACTORY_CLASS =
          ClassConfOption.create("giraph.edgeIdFactoryClass",
                  DefaultEdgeIdFactory.class, EdgeIdFactory.class,
                  "Edge ID Factory class - optional");
}
