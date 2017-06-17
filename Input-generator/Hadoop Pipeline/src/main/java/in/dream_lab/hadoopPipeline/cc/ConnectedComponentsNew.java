package in.dream_lab.hadoopPipeline.cc;

import java.io.*;
import java.util.*;

public class ConnectedComponentsNew {
   public HashMap<Long, Integer> findWeak(Map<Long, HashMap<Long, IEdge>> graph) {

        // TODO: profile DFS/BFS based WCC detection for undirected graphs vs disjoint set implementation

        // initialize disjoint set
        DisjointSets<Long> ds = new DisjointSets<Long>(graph.keySet().size());
        for (Long vertex : graph.keySet()) {
            ds.addSet(vertex);
        }

        for (Long vertex : graph.keySet()) {

            Long source = vertex;
            for (Map.Entry<Long, IEdge> pair : graph.get(vertex).entrySet()) {

//                Long sink = sin;
                Long sink = pair.getKey();
                IEdge edge = pair.getValue();
                if (!(edge.checkRemote()))
                    ds.union(source, sink);

            }
        }
        // union edge pairs
//        for (Long edge : graph.va) {
//            @SuppressWarnings("unchecked")
//            Long source = (Long)edge.getSource();
//            @SuppressWarnings("unchecked")
//            Long sink = (Long)edge.getSink();
//
//            ds.union(source, sink);
//        }

//        return
//   ds.retrieveSets();

//        System.out.println("Ans " + ds.numSets());

        int count = 0;
        HashMap<Long, Integer> cc = new HashMap<Long, Integer>();
        for (Collection<Long> subgraph : ds.retrieveSets()) {
            count++;
            for (Long v : subgraph) {
                cc.put(v, count);
            }
        }
        return cc;
    } 
}
