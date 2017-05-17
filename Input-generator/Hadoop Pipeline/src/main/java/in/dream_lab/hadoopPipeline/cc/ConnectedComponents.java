package in.dream_lab.hadoopPipeline.cc;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;

/* Method to find WCC
 * 
 * i/p : Adjacency list of the graph
 * 
 * o/p : map of <V,SGi> where V is vertexId & SGi is the subgraph to which it belongs
 * 
 * Data Interpretation: 
 * SubgraphId Int.MIN_VAL  indicates vertex yet to be visited
 * SubgraphId 0  indicates vertex is added to frontierQueue
 * SubgraphId >0  indicates vertex is assigned with subgraphID (valid)
 *  
 */

public class ConnectedComponents { // TODO:explicit WCC

    Map<Long,Integer> cc=new HashMap<Long,Integer>(); //A map containing <Vid,SGid> mapping //TODO: use value to store "visited" vertex using special value, e.g. -1 for SGID // TODO: Long:Int
    //HashMap<Long,Boolean> isVisited=new HashMap<Long,Boolean>(); //A map having flag for each vertex indicating whether it is visited or not // TODO: replace with above overloaded
    
    Map<Long,Boolean>toVisitQ=new HashMap<Long,Boolean>();
    
    private LinkedList<Long> frontierQueue =new LinkedList<Long>(); //TODO: rename as toVisitQueue/frontierQueue //TODO: linked list of some lighter q impl
	int sgid=0;

	//Multimap<Long, Long> mhm = HashMultimap.create();
	
	
	//TODO: Replace the linear search mechanism with efficient mechanism
	Long firstzeroElement(){ // TODO: return Long
		//System.out.println("TEST:first0 called");
//		for(Long k : cc.keySet()){
//			
//			//System.out.println("TESTKEY1st0:  "+k);
//			if (cc.get(k)==Integer.MIN_VALUE){
//				//System.out.println(" FIRST0 :Returned "+k);
//				return k;
//			}
//		}
//
//		return null; // TODO: return NULL 

		if(toVisitQ.isEmpty()){
			return null;
		}else{

			Map.Entry<Long,Boolean> entry=toVisitQ.entrySet().iterator().next();
			Long key= entry.getKey();
			toVisitQ.remove(key);
			return key;
		}
	
	}
	
	
	public Map<Long,Integer> findCC(Map <Long, HashMap<Long,IEdge>> AdjacencyMatrix){ // TODO: return Long:Int
		
		//this.mhm = mhm2;
		for (Object  key  :AdjacencyMatrix.keySet()){
			//System.out.println("TESTKEYINCC "+key);
			cc.put((Long)key,Integer.MIN_VALUE);
			toVisitQ.put((Long)key,true);
			//isVisited.put((Long)key, false); // TODO: cc.out(..., Int.MIN_VAL)
			
		}
		
	//	System.out.println("TEST:CC formed");
		Long parent;
				
		while (( parent=firstzeroElement()) != null ){//the above function does not returns NULL // TODO: should retunr NULL
		//	 firstzeroElement();
			
			sgid++; 
			
		
			frontierQueue.add(parent);
			cc.put(parent, 0); // cc.put(..., VISITED_FLAG)
			 
		//	while ((parent=((LinkedList<Long>) frontierQueue).removeFirst()) !=null){ // TODO: (parent = queue.get()) != null
			while (! ((LinkedList<Long>) frontierQueue).isEmpty()){
			// TODO: single operation to get and remove top element
				 //frontierQueue.remove(parent);
				parent=((LinkedList<Long>) frontierQueue).removeFirst();
		//		 System.out.println("Removed from queue "+parent);
				
				 cc.put(parent, sgid);
				
				 if(!(AdjacencyMatrix.get(parent).isEmpty())){
				 
					 HashMap<Long,IEdge> childlist =  AdjacencyMatrix.get(parent);
				 
				 /*for (Long s : childlist) {
					    System.out.println("TESTSET  " +s);
					}
				 */
				 
				 for( Map.Entry<Long,IEdge> entry : childlist.entrySet()){
					// System.out.println("TESTCHILD "+child+" "+ cc.get(child)+ " "+parent+" "+ cc.get(parent));
					 if(entry!=null &&  (!( entry.getValue().checkRemote() )) && (cc.get(entry.getKey()) == Integer.MIN_VALUE)){ // TODO
						 ((LinkedList<Long>) frontierQueue).addLast(entry.getKey());					//		queue.add(child);
						 //isVisited.put(child, true); // TODO
						 cc.put(entry.getKey(), 0); //indicates vertex is added to frontierQueue
						 toVisitQ.remove(entry.getKey());
					//System.out.println("Added to queue "+child +":"+cc.get(child));	 
					 }
					 
					 
				 }
				 }
				}
			// System.out.println("TEST: Queue is empty");
			 }
		System.out.println("TEST: SGID "+sgid);
		//AdjacencyMatrix.clear();
		return cc;
			
		}
}